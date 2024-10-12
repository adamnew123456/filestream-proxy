using System;
using System.Collections;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace filestream_proxy
{
    static class Extensions
    {
        /// Reads continuously until the buffer is filled.
        public static void ReadAll(this Stream stream, byte[] array)
        {
            stream.ReadAll(array, 0, array.Length);
        }
        
        /// Reads continuously until the designated slice of the buffer is filled.
        public static void ReadAll(this Stream stream, byte[] array, int offset, int length)
        {
            while (length > 0)
            {
                var read = stream.Read(array, offset, length);
                if (read == 0)
                    throw new EndOfStreamException($"Expected {length} more bytes in the stream, read {offset}");

                offset += read;
                length -= read;
            }
        }
    }
    
    /// <summary>
    /// A lock that is represented by a file on the filesystem. This can only be created by the
    /// Acquire method, which ensures the file does not already exist. Disposing of this object
    /// releases the lock by deleting the file.
    /// </summary>
    sealed class LockFile : IDisposable
    {
        private readonly string _path;

        private LockFile(string path)
        {
            _path = path;
        }

        /// Attempts to create the file. If successful a LockFile is returned, otherwise null is
        /// returned.
        public static LockFile? Acquire(string path)
        {
            try
            {
                File.Open(path, FileMode.CreateNew, FileAccess.Write).Dispose();
                return new LockFile(path);
            }
            catch (IOException)
            {
                return null;
            }
        }

        public void Dispose()
        {
            File.Delete(_path);
        }
    }

    /// A pipe file transfers ordered data between two processes. The file is a series of fixed-size pages,
    /// each of which can be either used (containing data and an ordering number) or unused. Each page also
    /// has an order so the reader and writer can each determine what page they should act on next.
    sealed class PipeFile : IDisposable
    {
        public const int PageCount = 64;
        public const int PageCapacity = 8192;
        
        private readonly byte[] _fileMagic = Encoding.ASCII.GetBytes("Pipe");
        private const int PageHeaderSize = sizeof(uint) * 2; // 4 bytes for size, 4 bytes for serial
        
        private const int FileSize = 4 + PageCount * (PageHeaderSize + PageCapacity);

        private const uint SerialUnused = 0;
        private const uint EmptySize = 0;
        private const uint TerminateSize = 0;
        
        private readonly FileStream _stream;
        private readonly uint[] _pageSerials = new uint[PageCount];
        private readonly uint[] _pageSizes = new uint[PageCount];
        
        // Cache, usually you operate on the same page several times in a row
        private int _lastPage = -1;
        private uint _lastSerial = SerialUnused;

        /// Opens a pipe file at the given path. An empty or corrupt file is initialized with a
        /// file header, while an existing pipe file is read as-is.
        public PipeFile(string path)
        {
            _stream = File.Open(path, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            if (!InitPagesIfNeeded())
                ReadPageStates();
        }

        /// Finds a page in the file that is not currently used. Returns true if a free page was
        /// found, and false otherwise.
        public bool AllocatePage(uint serial)
        {
            for (var i = 0; i < PageCount; i++)
            {
                if (_pageSerials[i] == SerialUnused)
                {
                    UpdatePageHeader(i, serial, EmptySize, true);
                    return true;
                }
            }
            return false;
        }
        
        /// Marks the given page as unused. Does nothing if the given page does not exist. 
        public void ReleasePage(uint serial)
        {
            if (!TryFindPage(serial, out var page))
                return;
            
            UpdatePageHeader(page, SerialUnused, EmptySize, true);
        }
        
        /// Writes a termination page. Upon reading a termination page, the reader will stop
        /// reading.
        public void WriteTermination(uint serial)
        {
            if (!TryFindPage(serial, out var page))
                return;
            
            UpdatePageHeader(page, serial, TerminateSize, true);
        }
        
        /// Writes data to the given page. Returns the amount of bytes written, which may be less
        /// than the buffer size because each page has a fixed size. Length must be at least zero
        /// otherwise an exception will be thrown.
        ///
        /// Note that this does not support partial writes - calling this method twice on the same
        /// page will overwrite all data previously written to the page.
        public int WritePage(uint serial, byte[] buffer, int offset, int length)
        {
            if (!TryFindPage(serial, out var page))
                return 0;
            
            var writeLength = Math.Min(length, PageCapacity);

            UpdatePageHeader(page, serial, (uint) writeLength, true);
            
            var pageDataOffset = _fileMagic.Length + (PageCount * PageHeaderSize) + (page * PageCapacity);
            _stream.Seek(pageDataOffset, SeekOrigin.Begin);
            _stream.Write(buffer, offset, writeLength);
            return writeLength;
        }
        
        /// Reads the data from the page into the given buffer. This reads the *entire* page, so
        /// the buffer free space must be as large as the page capacity. Returns the number of
        /// bytes read or zero if no page matching the given serial exists.
        ///
        /// If the last page is a termination page, a negative value is returned. 
        public int ReadPage(uint serial, byte[] buffer, int offset)
        {
            var length = buffer.Length - offset;
            if (length < PageCapacity)
                throw new ArgumentException($"Buffer capacity {buffer.Length - offset} not large enough for page");
            
            if (!TryFindPage(serial, out var page))
                return 0;

            var readLength = (int) _pageSizes[page];
            if (readLength == TerminateSize)
                return -1;

            var pageDataOffset = _fileMagic.Length + (PageCount * PageHeaderSize) + (page * PageCapacity);
            _stream.Seek(pageDataOffset, SeekOrigin.Begin);
            
            _stream.ReadAll(buffer, offset, readLength);
            return readLength;
        }

        /// Initializes the pages within the file, if the file is not already initialized.
        /// Returns true if the file was just initialized, and false if the file was already
        /// initialized.
        private bool InitPagesIfNeeded()
        {
            var pos = _stream.Seek(0, SeekOrigin.End);
            _stream.Seek(0, SeekOrigin.Begin);

            if (pos == FileSize)
            {
                var magic = new byte[4];
                _stream.ReadAll(magic);
                if (StructuralComparisons.StructuralEqualityComparer.Equals(_fileMagic, magic))
                    return false;
            }

            // If the stream size is different than expected, we need to reinitialize the page headers.
            _stream.SetLength(FileSize);
            _stream.Seek(_fileMagic.Length, SeekOrigin.Begin);
            for (var i = 0; i < PageCount; i++)
            {
                UpdatePageHeader(i, SerialUnused, EmptySize, false);
            }
            
            // Write magic last, that way a partially initialized file does not look completely initialized
            _stream.Seek(0, SeekOrigin.Begin);
            _stream.Write(_fileMagic);
            return true;
        }
        
        /// Reads all the page headers and builds arrays containing the page serial numbers and sizes.
        private void ReadPageStates()
        {
            _stream.Seek(_fileMagic.Length, SeekOrigin.Begin);
            var headerBuffer = new byte[PageCount * PageHeaderSize];
            _stream.ReadAll(headerBuffer);
            
            for (var i = 0; i < PageCount; i++)
            {
                _pageSerials[i] = BitConverter.ToUInt16(headerBuffer, i * PageHeaderSize);
                _pageSizes[i] = BitConverter.ToUInt16(headerBuffer, i * PageHeaderSize + sizeof(uint));
            }
        }

        /// Find the index of the page with the given serial number. Returns true if found and false otherwise.
        private bool TryFindPage(uint serial, out int page)
        {
            if (_lastSerial == serial)
            {
                page = _lastPage;
                return true;
            }
            
            for (page = 0; page < PageCount; page++)
            {
                if (_pageSerials[page] == serial)
                {
                    _lastSerial = serial;
                    _lastPage = page;
                    return true;
                }
            }
            return false;
        }

        /// Sets the serial and size for the given page, and writes it to disk.
        private void UpdatePageHeader(int page, uint serial, uint size, bool needSeek)
        {
            if (needSeek)
            {
                var pageHeaderOffset = _fileMagic.Length + (page * PageHeaderSize);
                _stream.Seek(pageHeaderOffset, SeekOrigin.Begin);
            }
            _stream.Write(BitConverter.GetBytes(serial));
            _stream.Write(BitConverter.GetBytes(size));
            _pageSerials[page] = serial;
            _pageSizes[page] = size;
        }

        public void Dispose()
        {
            _stream.Flush();
            _stream.Dispose();
        }
    }

    /// Copies data from a socket into the pipe file.
    sealed class ReaderThread
    {
        private string _lockFilePath;
        private string _pipeFilePath;
        private Socket _socket;
        private AutoResetEvent _completion;
        private volatile bool _stop;

        public ReaderThread(string lockFilePath, string pipeFile, Socket socket, AutoResetEvent completion)
        {
            _lockFilePath = lockFilePath;
            _pipeFilePath = pipeFile;
            _socket = socket;
            _completion = completion;
        }

        public void Abort()
        {
            _stop = true;
        }

        public void RunAndNotifyComplete()
        {
            Run();
            Console.WriteLine($"Reader: Signal done");
            _completion.Set();
        }

        private void Run()
        {
            var buffer = new byte[PipeFile.PageCapacity];
            var delay = TimeSpan.FromMilliseconds(50);
            bool terminating = false;
            uint nextToWrite = 1;
            while (true)
            {
                if (_stop)
                    return;

                int read = 0;
                try
                {
                    read = _socket.Receive(buffer);
                    if (read == 0)
                        terminating = true;
                }
                catch (SocketException err)
                {
                    if (err.SocketErrorCode == SocketError.TimedOut)
                        continue;
                    Console.WriteLine($"Dying over reader IO exception: {err.Message}");
                    terminating = true;
                }
                catch (IOException err)
                {
                    terminating = true;
                }

                var wrotePage = false;
                var idle = false;
                while (!wrotePage)
                {
                    if (_stop) 
                        return;
                    if (idle)
                    {
                        Thread.Sleep(delay);
                        idle = false;
                    }

                    LockFile? lockFile = LockFile.Acquire(_lockFilePath);
                    if (lockFile == null)
                    {
                        idle = true;
                        continue;
                    }
                    
                    using (lockFile)
                    {
                        using var pipeFile = new PipeFile(_pipeFilePath);
                        if (!pipeFile.AllocatePage(nextToWrite))
                        {
                            idle = true;
                            continue;
                        }

                        if (terminating)
                        {
                            Console.WriteLine($"Reader is terminating");
                            pipeFile.WriteTermination(nextToWrite);
                        }
                        else
                            pipeFile.WritePage(nextToWrite, buffer, 0, read);
                        nextToWrite++;
                        wrotePage = true;

                        if (terminating)
                            return;
                    }
                }
            }
        }
    }
    
    /// Copies data from a pipe file into a socket.
    sealed class WriterThread
    {
        private string _lockFilePath;
        private string _pipeFilePath;
        private Socket _socket;
        private AutoResetEvent _completion;
        private volatile bool _stop;

        public WriterThread(string lockFilePath, string pipeFile, Socket socket, AutoResetEvent completion)
        {
            _lockFilePath = lockFilePath;
            _pipeFilePath = pipeFile;
            _socket = socket;
            _completion = completion;
        }

        public void Abort()
        {
            _stop = true;
        }
        
        public void RunAndNotifyComplete()
        {
            Run();
            Console.WriteLine($"Writer: signal completion");
            _completion.Set();
        }

        private void Run()
        {
            var buffer = new byte[PipeFile.PageCapacity];
            var delay = TimeSpan.FromMilliseconds(50);
            uint nextToRead = 1;
            while (true)
            {
                var toWrite = 0;
                var idle = false;
                while (toWrite == 0)
                {
                    if (_stop) 
                        return;
                    if (idle)
                    {
                        Thread.Sleep(delay);
                        idle = false;
                    }
                    LockFile? lockFile = LockFile.Acquire(_lockFilePath);
                    if (lockFile == null)
                    {
                        idle = true;
                        continue;
                    }
                    
                    using (lockFile)
                    {
                        using var pipeFile = new PipeFile(_pipeFilePath);
                        toWrite = pipeFile.ReadPage(nextToRead, buffer, 0);
                        if (toWrite == 0)
                        {
                            idle = true;
                            continue;
                        }
                        else if (toWrite == -1)
                        {
                            Console.WriteLine($"Writer hit termination page");
                            return;
                        }
                        
                        pipeFile.ReleasePage(nextToRead);
                        nextToRead++;
                    }
                }

                try
                {
                    _socket.Send(buffer, 0, toWrite, SocketFlags.None);
                }
                catch (IOException err)
                {
                    Console.WriteLine($"Dying over writer IO exception: {err.Message}");
                    return;
                }
            }
        }
    }
    
    internal class Program
    {
        private const string READ_LOCK_FILENAME = "rlock";
        private const string WRITE_LOCK_FILENAME = "wlock";
        private const string READ_PIPE_FILENAME = "rpipe";
        private const string WRITE_PIPE_FILENAME = "wpipe";
        
        private static void Usage(string context)
        {
            Console.WriteLine("Usage: filestream-proxy (listen DIRECTORY PORT | forward DIRECTORY IP:PORT)");
            Console.WriteLine(context);
            Environment.Exit(1);
        }

        private static void Listen(string directory, int port)
        {
            var listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            Console.WriteLine($"Listening on localhost port {port}");

            Console.WriteLine("Waiting for client...");
            using (var client = listener.AcceptSocket())
            {
                client.ReceiveTimeout = 1000;
                
                var completion = new AutoResetEvent(false);
                var reader = new ReaderThread(Path.Combine(directory, READ_LOCK_FILENAME),
                    Path.Combine(directory, READ_PIPE_FILENAME),
                    client,
                    completion);
                var writer = new WriterThread(Path.Combine(directory, WRITE_LOCK_FILENAME),
                    Path.Combine(directory, WRITE_PIPE_FILENAME),
                    client,
                    completion);
                    
                Console.WriteLine("Starting listener IO threads...");
                var readerThread = new Thread(reader.RunAndNotifyComplete);
                var writerThread = new Thread(writer.RunAndNotifyComplete);
                readerThread.Start();
                writerThread.Start();
                completion.WaitOne();
                
                Console.WriteLine("Listener IO thread has stopped, aborting...");
                reader.Abort();
                writer.Abort();
                Console.WriteLine("Waiting for reader thread...");
                readerThread.Join();
                Console.WriteLine("Waiting for writer thread...");
                writerThread.Join();
            }
        }
        
        private static void Forward(string directory, IPEndPoint endpoint)
        {
            using (var client = new Socket(SocketType.Stream, ProtocolType.Tcp))
            {
                Console.WriteLine($"Connecting to {endpoint}");
                client.Connect(endpoint);
                
                Console.WriteLine($"Connected, forwarding traffic");
                client.ReceiveTimeout = 1000;
                
                var completion = new AutoResetEvent(false);
                var reader = new ReaderThread(Path.Combine(directory, WRITE_LOCK_FILENAME),
                    Path.Combine(directory, WRITE_PIPE_FILENAME),
                    client,
                    completion);
                var writer = new WriterThread(Path.Combine(directory, READ_LOCK_FILENAME),
                    Path.Combine(directory, READ_PIPE_FILENAME),
                    client,
                    completion);
                    
                Console.WriteLine("Starting forwarder IO threads...");
                var readerThread = new Thread(reader.RunAndNotifyComplete);
                var writerThread = new Thread(writer.RunAndNotifyComplete);
                readerThread.Start();
                writerThread.Start();
                completion.WaitOne();
                
                Console.WriteLine("Forwarder IO thread has stopped, aborting...");
                reader.Abort();
                writer.Abort();
                Console.WriteLine("Waiting for reader thread...");
                readerThread.Join();
                Console.WriteLine("Waiting for writer thread...");
                writerThread.Join();
            }
        }
        
        public static void Main(string[] args)
        {
            if (args.Length == 0)
                Usage("Either 'listen' or 'forward' is required");

            switch (args[0])
            {
                case "listen":
                    Listen(args[1], int.Parse(args[2]));
                    break;
                case "forward":
                    Forward(args[1], IPEndPoint.Parse(args[2]));
                    break;
                default:
                    Usage($"Unknown command: {args[0]}");
                    break;
            }
        }
    }
}
