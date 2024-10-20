using System.Buffers;
using System.Collections;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace filestream_proxy
{
    static class Extensions
    {
        /// Reads continuously until the buffer is filled.
        public static void ReadAll(this Stream stream, Span<byte> array)
        {
            var offset = 0;
            var length = array.Length;
            while (length > 0)
            {
                var read = stream.Read(array.Slice(offset, length));
                if (read == 0)
                    throw new EndOfStreamException($"Expected {length} more bytes in the stream, read {offset}");

                offset += read;
                length -= read;
            }
        }
        
        /// Sends repeatedly until the socket has accepted the entire span.
        public static void SendAll(this Socket socket, ReadOnlySpan<byte> span)
        {
            var offset = 0;
            var toSend = span.Length;
            while (toSend > 0)
            {
                var sent = socket.Send(span.Slice(offset, toSend));
                offset += sent;
                toSend -= sent;
            }
        }
    }
    
    /// A lock represented by a file on the filesystem. This can only be created by the
    /// Acquire method, which ensures the file does not already exist. Disposing of this object
    /// releases the lock by deleting the file.
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

    /// Metadata about each page in a pipe file, indicating the ordering of the page and the size
    /// of its contents.
    sealed class PageHeader
    {
        /// Size of the page header, in bytes, after it is serialized.
        public const int TotalSize = sizeof(ulong) + sizeof(uint);
        
        /// Special serial number value used for unallocated pages.
        public const ulong SerialUnused = 0;
        
        /// The order in which this page should be processed. 1 is processed first, then 2, etc.
        public ulong Serial { get; init; }
        
        /// The amount of data in the page. May be 0 if the command is for connection control.
        public uint Size { get; init; }
        
        /// A header for an unallocated page.
        public static readonly PageHeader Blank = new PageHeader
        {
            Serial = SerialUnused,
            Size = 0
        };

        /// Reads the page header from the provided buffer.
        public static PageHeader Read(Span<byte> buffer, out int bytesRead)
        {
            var serial = BitConverter.ToUInt32(buffer);
            bytesRead = sizeof(ulong);

            var size = BitConverter.ToUInt32(buffer[bytesRead..]);
            bytesRead += sizeof(uint);

            if (serial == SerialUnused && size == 0)
                return Blank;
            
            return new PageHeader
            {
                Serial = serial,
                Size = size
            };
        }

        /// Writes the page header into the provided buffer. Returns the amount of data written.
        public int Write(Span<byte> buffer)
        {
            var offset = 0;
            BitConverter.TryWriteBytes(buffer[offset..], Serial);
            offset += sizeof(ulong);
            
            BitConverter.TryWriteBytes(buffer[offset..], Size);
            offset += sizeof(uint);
            
            return offset;
        }
    }

    /// Common parameters required to use a PipeFile.
    sealed class PipeFileConfig
    {
        /// The bytes at the start that identify a pipe file.
        public static readonly byte[] FileMagic = Encoding.ASCII.GetBytes("Pipe");
        
        /// The path of the lock file that governs access to the pipe.
        public string LockPath { get; init; }
        
        /// The path of the pipe file itself.
        public string PipePath { get; init; }
        
        /// The number of pages that fit in the pipe.
        public int PageCount { get; init; }
        
        /// The amount of data that fits in each page, not including the size of the header.
        public int PageCapacity { get; init; }

        /// The total size of the file on disk.
        public int FileSize => FileMagic.Length + PageCount * (PageHeader.TotalSize + PageCapacity);

        /// Compute the offset to a specific header within the file.
        public int HeaderOffset(int page) => FileMagic.Length + (PageHeader.TotalSize * page);
        
        /// Compute the offset to a specific data block within the file.
        public int DataOffset(int page) => FileMagic.Length + (PageHeader.TotalSize * PageCount) + (PageCapacity * page);

        /// Removes a stale lock.
        public void Clean()
        {
            File.Delete(LockPath);
            File.Delete(PipePath);
        }
    }

    /// A pipe file transfers ordered data between two processes. The file is a series of fixed-size pages,
    /// each of which can be either used (containing data and an ordering number) or unused. Each page also
    /// has an order so the reader and writer can each determine what page they should act on next.
    sealed class PipeFile : IDisposable
    {
        private readonly FileStream _stream;
        private readonly PipeFileConfig _config;
        private readonly PageHeader[] _headers;
        private readonly LockFile _lockFile;
        
        // Cache, usually you operate on the same page several times in a row
        private int _lastPage = -1;
        private ulong _lastSerial = PageHeader.SerialUnused;
        
        /// Finds a free page in the pipe and initializes it with the provided contents. If no
        /// page can be found immediately, it is retried after sleeping for each check interval. 
        public static Task AllocateAndWriteAsync(PipeFileConfig config, ulong serial, ReadOnlyMemory<byte> buffer, TimeSpan checkInterval, CancellationToken? cancel)
        {
            if (buffer.Length > config.PageCapacity)
                throw new ArgumentException($"Buffer capacity to large: {buffer.Length} > {config.PageCapacity}");
            
            return Task.Run(() =>
            {
                var header = new PageHeader
                {
                    Serial = serial,
                    Size = (uint) buffer.Length
                };
                
                var delay = false;
                while (true)
                {
                    if (cancel != null && cancel.Value.IsCancellationRequested)
                        throw new TaskCanceledException();
                    
                    if (delay)
                        Thread.Sleep(checkInterval);
                    delay = true;

                    var lockFile = LockFile.Acquire(config.LockPath);
                    if (lockFile == null) continue;

                    using var pipeFile = new PipeFile(lockFile, config);
                    if (!pipeFile.AllocatePage(header)) continue;
                    pipeFile.WritePage(serial, buffer.Span);
                    break;
                }
            });
        }
        
        /// Finds the page in the pipe for the provided serial and reads it into the given buffer.
        /// If no page representing that serial is present, this sleeps for the check interval
        /// and retries until the page is found.  
        public static Task<PageHeader> ReadAndReleaseAsync(PipeFileConfig config, ulong serial, Memory<byte> buffer, TimeSpan checkInterval, CancellationToken? cancel)
        {
            if (buffer.Length < config.PageCapacity)
                throw new ArgumentException($"Buffer capacity to small: {buffer.Length} < {config.PageCapacity}");
            
            return Task.Run(() =>
            {
                var delay = false;
                while (true)
                {
                    if (cancel != null && cancel.Value.IsCancellationRequested)
                        throw new TaskCanceledException();
                        
                    if (delay)
                        Thread.Sleep(checkInterval);
                    delay = true;

                    var lockFile = LockFile.Acquire(config.LockPath);
                    if (lockFile == null) continue;

                    using var pipeFile = new PipeFile(lockFile, config);
                    if (!pipeFile.ReadPage(serial, buffer.Span, out var header))
                        continue;
                    
                    pipeFile.ReleasePage(serial);
                    return header;
                }
            });
        }

        /// Opens a pipe file at the given path. An empty or corrupt file is initialized with a
        /// file header, while an existing pipe file is read as-is.
        private PipeFile(LockFile lockFile, PipeFileConfig config)
        {
            _lockFile = lockFile;
            _config = config;
            _stream = File.Open(config.PipePath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            _headers = new PageHeader[config.PageCount];
            if (!InitPagesIfNeeded())
                ReadPageStates();
        }

        /// Finds a page in the file that is not currently used. Returns true if a free page was
        /// found, and false otherwise.
        private bool AllocatePage(PageHeader header)
        {
            for (var i = 0; i < _config.PageCount; i++)
            {
                if (_headers[i].Serial == PageHeader.SerialUnused)
                {
                    _lastPage = i;
                    _lastSerial = header.Serial;
                    UpdatePageHeader(i, header, true);
                    return true;
                }
            }
            return false;
        }
        
        /// Marks the given page as unused. Does nothing if the given page does not exist. 
        private void ReleasePage(ulong serial)
        {
            if (!TryFindPage(serial, out var page))
                return;
            
            UpdatePageHeader(page, PageHeader.Blank, true);
        }
        
        /// Reads the data from the page into the given buffer. This reads the *entire* page, so
        /// the buffer free space must be as large as the page capacity. Returns the number of
        /// bytes read or zero if no page matching the given serial exists.
        private bool ReadPage(ulong serial, Span<byte> buffer, out PageHeader header)
        {
            if (buffer.Length < _config.PageCapacity)
                throw new ArgumentException($"Buffer capacity to small: {buffer.Length} < {_config.PageCapacity}");

            if (!TryFindPage(serial, out var page))
            {
                header = PageHeader.Blank;
                return false;
            }

            header = _headers[page];
            var readLength = (int)header.Size;
            if (readLength > 0)
            {
                _stream.Seek(_config.DataOffset(page), SeekOrigin.Begin);
                _stream.ReadAll(buffer);
            }
            return true;
        }
        
        /// Writes data to the given page. Returns the amount of bytes written, which may be less
        /// than the buffer size because each page has a fixed size. Length must be at least zero
        /// otherwise an exception will be thrown.
        ///
        /// Note that this does not support partial writes - calling this method twice on the same
        /// page will overwrite all data previously written to the page.
        private void WritePage(ulong serial, ReadOnlySpan<byte> buffer)
        {
            if (!TryFindPage(serial, out var page))
                return;
            
            if (buffer.Length > _config.PageCapacity)
                throw new ArgumentException($"Buffer too large: {buffer.Length} > {_config.PageCapacity}");
            
            _stream.Seek(_config.DataOffset(page), SeekOrigin.Begin);
            _stream.Write(buffer);
        }
        
        /// Initializes the pages within the file, if the file is not already initialized.
        /// Returns true if the file was just initialized, and false if the file was already
        /// initialized.
        private bool InitPagesIfNeeded()
        {
            var pos = _stream.Seek(0, SeekOrigin.End);
            _stream.Seek(0, SeekOrigin.Begin);

            if (pos == _config.FileSize)
            {
                var magic = new byte[PipeFileConfig.FileMagic.Length];
                _stream.ReadAll(magic);
                if (StructuralComparisons.StructuralEqualityComparer.Equals(PipeFileConfig.FileMagic, magic))
                    return false;
            }

            // If the stream size is different than expected, we need to reinitialize the page headers.
            _stream.SetLength(_config.FileSize);
            _stream.Seek(_config.HeaderOffset(0), SeekOrigin.Begin);
            for (var i = 0; i < _config.PageCount; i++)
            {
                UpdatePageHeader(i, PageHeader.Blank, false);
            }
            
            // Write magic last, that way a partially initialized file does not look completely initialized
            _stream.Seek(0, SeekOrigin.Begin);
            _stream.Write(PipeFileConfig.FileMagic);
            return true;
        }
        
        /// Reads all the page headers and builds arrays containing the page serial numbers and sizes.
        private void ReadPageStates()
        {
            _stream.Seek(_config.HeaderOffset(0), SeekOrigin.Begin);
            var headerBuffer = new byte[_config.PageCount * PageHeader.TotalSize];
            _stream.ReadAll(headerBuffer);

            var offset = 0;
            for (var i = 0; i < _config.PageCount; i++)
            {
                _headers[i] = PageHeader.Read(headerBuffer.AsSpan()[offset..], out var read);
                offset += read;
            }
        }

        /// Find the index of the page with the given serial number. Returns true if found and false otherwise.
        private bool TryFindPage(ulong serial, out int page)
        {
            if (_lastSerial == serial)
            {
                page = _lastPage;
                return true;
            }
            
            for (page = 0; page < _config.PageCount; page++)
            {
                if (_headers[page].Serial != serial) continue;
                _lastSerial = serial;
                _lastPage = page;
                return true;
            }
            return false;
        }

        /// Sets the serial and size for the given page, and writes it to disk.
        private void UpdatePageHeader(int page, PageHeader header, bool needSeek)
        {
            if (needSeek)
                _stream.Seek(_config.HeaderOffset(page), SeekOrigin.Begin);

            var buffer = ArrayPool<byte>.Shared.Rent(PageHeader.TotalSize);
            var wrote = header.Write(buffer);
            _stream.Write(buffer.AsSpan()[..wrote]);
            ArrayPool<byte>.Shared.Return(buffer);

            _headers[page] = header;
        }

        public void Dispose()
        {
            _stream.Flush();
            _stream.Dispose();
            _lockFile.Dispose();
        }
    }
    
    /// Commands that can be sent over the command pipe.
    enum ControlCommand : byte
    {
        /// Should not be used, only visible when a page has not been written to.
        None = 0,
        /// Instructs the client end to connect to the remote, and assign the connection the ID
        /// in the page header. Any data in the page is discarded.
        Connect,
        /// Instructs the peer (client or server can send this) to close the connection. Any data
        /// in the page is discarded.
        Close
    }
    
    /// Commands that can be sent over the command pipe.
    sealed class ControlMessage
    {
        /// Size of the message after it is serialized.
        public const int TotalSize = sizeof(byte) * 2;
        
        /// What to do with the connection
        public ControlCommand Command { get; init; }
        
        /// The connection to act on
        public byte ConnectionId { get; init; }

        /// Reads the page header from the provided buffer.
        public static ControlMessage Read(Span<byte> buffer)
        {
            var commandRaw = buffer[0];
            var command = ControlCommand.None;
            if (Enum.IsDefined(typeof(ControlCommand), commandRaw))
                command = (ControlCommand) commandRaw;

            return new ControlMessage
            {
                Command = command,
                ConnectionId = buffer[1]
            };
        }

        /// Writes the command into the provided buffer. Returns the amount of data written.
        public void Write(Span<byte> buffer)
        {
            buffer[0] = (byte) Command;
            buffer[1] = ConnectionId;
        }
    }

    /// Basic interface for connection workers to use when cancelling themselves and checking their status.
    interface IConnectionMaster
    {
        /// Cancellation used to stop the connection worker.
        public CancellationToken Token { get; }

        /// Request that the connection worker stops. May be called from inside the worker.
        public void Cancel(string reason);
    }

    /// Passes data between a socket and a read/write pipe file.
    sealed class ConnectionWorker
    {
        private static readonly TimeSpan CheckInterval = TimeSpan.FromMilliseconds(50);
        
        private Socket _socket;
        private IConnectionMaster _master;
        private PipeFileConfig _readPipe;
        private PipeFileConfig _writePipe;
        private long _bytesRead = 0;
        private long _bytesWritten = 0;

        public ConnectionWorker(Socket socket, IConnectionMaster master, PipeFileConfig readPipe, PipeFileConfig writePipe)
        {
            _socket = socket;
            _master = master;
            _readPipe = readPipe;
            _writePipe = writePipe;
        }

        /// Passes data from the socket to the read pipe, and from the write pipe onto the socket. Stops when the cancellation trips or an exception occurs.
        public void Run()
        {
            ulong readSerial = PageHeader.SerialUnused + 1;
            var readBuffer = new byte[_readPipe.PageCapacity];
            var readerTask = ReaderTask(readSerial, readBuffer);
            
            ulong writeSerial = PageHeader.SerialUnused + 1;
            var writeBuffer = new byte[_readPipe.PageCapacity];
            var writerTask = WriterTask(writeSerial, writeBuffer);

            var timer = Stopwatch.StartNew();
            var connectionId = $"<connection {_socket.LocalEndPoint} {_socket.RemoteEndPoint}>";
            while (true)
            {
                if (_master.Token.IsCancellationRequested) break;

                try
                {
                    var action = Task.WaitAny(readerTask, writerTask);
                    if (action == 0)
                    {
                        var nextSerial = readerTask.Result;
                        if (nextSerial != null)
                            readSerial = nextSerial.Value;
                        else
                            Console.WriteLine($"{connectionId} cancelling due to empty read");
                        readerTask = ReaderTask(readSerial, readBuffer);
                    }
                    else
                    {
                        writeSerial = writerTask.Result;
                        writerTask = WriterTask(writeSerial, writeBuffer);
                    }
                }
                catch (Exception err)
                {
                    if (err is AggregateException)
                        err = ((AggregateException)err).InnerExceptions[0];
                    Console.WriteLine($"{connectionId} received error {err.Message}, closing");
                    _master.Cancel("connection exception");
                }

                if (timer.ElapsedMilliseconds > 5000)
                {
                    Console.WriteLine($"{connectionId} ms={timer.ElapsedMilliseconds} read={Interlocked.Exchange(ref _bytesRead, 0)} wrote={Interlocked.Exchange(ref _bytesWritten, 0)}");
                    timer.Restart();
                }
            }
            
            Console.WriteLine($"{connectionId} remainder read={Interlocked.Read(ref _bytesRead)} wrote={Interlocked.Read(ref _bytesWritten)}");
            _socket.Close();
        }

        /// Receives data from the socket and passes it into the read pipe.
        private async Task<ulong?> ReaderTask(ulong serial, byte[] buffer)
        {
            var read = await _socket.ReceiveAsync(buffer, SocketFlags.None, _master.Token);
            if (read == 0)
            {
                _master.Cancel("empty read");
                return null;
            }

            Interlocked.Add(ref _bytesRead, read);
            await PipeFile.AllocateAndWriteAsync(_readPipe, serial, buffer.AsMemory(0, read), CheckInterval, _master.Token);
            return serial + 1;
        }
        
        /// Reads data from the write pipe and sends it over the socket.
        private async Task<ulong> WriterTask(ulong serial, byte[] buffer)
        {
            var page = await PipeFile.ReadAndReleaseAsync(_writePipe, serial, buffer, CheckInterval, _master.Token);
            Interlocked.Add(ref _bytesWritten, page.Size);
            _socket.SendAll(buffer.AsSpan()[..(int)page.Size]);
            return serial + 1;
        }
    }

    /// Tracks a connection worker along with its cancellation and status.
    sealed class ConnectionContext : IConnectionMaster, IDisposable
    {
        private Thread _worker;
        private CancellationTokenSource _source;
        
        /// The token used to stop the connection worker.
        public CancellationToken Token { get; }
        
        /// Whether a close message has been sent to the peer.
        public bool PeerNotified { get; set; }

        public ConnectionContext()
        {
            _source = new CancellationTokenSource();
            Token = _source.Token;
        }

        /// Creates a new thread for the worker and starts it.
        public void Start(ConnectionWorker worker)
        {
            _worker = new Thread(worker.Run);
            _worker.Start();
        }
        
        /// Waits for the worker thread to stop.
        public void Join()
        {
            _worker.Join();
        }

        /// Asks the worker to stop itself.
        public void Cancel(string reason)
        {
            Console.WriteLine($"<cancel> explicitly requested by {reason}");
            _source.Cancel();
        }

        public void Dispose()
        {
            _source.Dispose();
        }
    }

    /// Base class for services that manage multiple connections.
    abstract class TunnelService
    {
        protected static readonly TimeSpan CheckInterval = TimeSpan.FromMilliseconds(50);
        private const int MaxConnections = byte.MaxValue;
        private const int ControlPageCount = 8;
        private const int ControlPageSize = ControlMessage.TotalSize;
        private const int WorkerPageCount = 64;
        private const int WorkerPageSize = 8192;

        private string _pipeDirectory;
        private PipeFileConfig _controlReadPipe;
        private PipeFileConfig _controlWritePipe;

        private ulong _readSerial = PageHeader.SerialUnused + 1;
        private ulong _writeSerial = PageHeader.SerialUnused + 1;
        protected ConnectionContext?[] _connections = new ConnectionContext?[MaxConnections];
        
        /// Creates a pipe for sending control messages.
        public static PipeFileConfig NewControlReadPipe(string pipeDirectory)
        {
            return new PipeFileConfig
            {
                LockPath = Path.Combine(pipeDirectory, "ctlrlock"),
                PipePath = Path.Combine(pipeDirectory, "ctlrpipe"),
                PageCount = ControlPageCount,
                PageCapacity = ControlPageSize
            };
        }
        
        /// Creates a pipe for sending control messages.
        public static PipeFileConfig NewControlWritePipe(string pipeDirectory)
        {
            return new PipeFileConfig
            {
                LockPath = Path.Combine(pipeDirectory, "ctlwlock"),
                PipePath = Path.Combine(pipeDirectory, "ctlwpipe"),
                PageCount = ControlPageCount,
                PageCapacity = ControlPageSize
            };
        }
        
        protected TunnelService(string pipeDirectory, PipeFileConfig controlReadPipe, PipeFileConfig controlWritePipe)
        {
            _pipeDirectory = pipeDirectory;
            _controlReadPipe = controlReadPipe;
            _controlWritePipe = controlWritePipe;
        }

        /// Finds a free connection and returns its ID, or returns null if no slots are free.
        protected byte? FindFreeConnection()
        {
            for (byte i = 0; i < _connections.Length; i++)
            {
                if (_connections[i] == null)
                    return i;
            }
            return null;
        }

        /// Send a control message over the tunnel's control pipe.
        protected void SendControlMessage(ControlCommand command, byte connectionId)
        {
            var readBuffer = new byte[_controlReadPipe.PageCapacity];
            var message = new ControlMessage
            {
                Command = command,
                ConnectionId = connectionId
            };

            message.Write(readBuffer);
            var writer = Task.Run(() =>
                PipeFile.AllocateAndWriteAsync(_controlReadPipe, _readSerial, readBuffer, CheckInterval, null));
            writer.Wait();
            _readSerial++;
        }

        /// Triggers the connection's cancellation token, and if necessary notifies the peer about the connection closure.
        protected void RequestConnectionClose(byte connectionId, string reason)
        {
            if (_connections[connectionId] == null) return;
            _connections[connectionId].Cancel(reason);
            
            if (_connections[connectionId].PeerNotified) return;
            _connections[connectionId].PeerNotified = true;
            SendControlMessage(ControlCommand.Close, connectionId);
        }
        
        /// Checks all valid connections and closes any whose cancellations have triggered.
        protected void WaitForCompletedConnections()
        {
            for (byte i = 0; i < _connections.Length; i++)
            {
                if (_connections[i] == null) continue;

                var cnx = _connections[i];
                if (cnx.Token.IsCancellationRequested)
                {
                    if (!cnx.PeerNotified)
                    {
                        cnx.PeerNotified = true;
                        SendControlMessage(ControlCommand.Close, i);
                    }
                    cnx.Join();
                    cnx.Dispose();
                    _connections[i] = null;
                }
            }
        }

        /// Creates a pipe for sending socket data.
        protected PipeFileConfig NewWorkerReadPipe(byte newConnectionId)
        {
            return new PipeFileConfig
            {
                LockPath = Path.Combine(_pipeDirectory, $"{newConnectionId}rlock"),
                PipePath = Path.Combine(_pipeDirectory, $"{newConnectionId}rpipe"),
                PageCount = WorkerPageCount,
                PageCapacity = WorkerPageSize
            };
        }
        
        /// Creates a pipe for sending socket data.
        protected PipeFileConfig NewWorkerWritePipe(byte newConnectionId)
        {
            return new PipeFileConfig
            {
                LockPath = Path.Combine(_pipeDirectory, $"{newConnectionId}wlock"),
                PipePath = Path.Combine(_pipeDirectory, $"{newConnectionId}wpipe"),
                PageCount = WorkerPageCount,
                PageCapacity = WorkerPageSize
            };
        }
        
        /// Reads a control message from the control pipe.
        protected async Task<ControlMessage> WriterTask()
        {
            var buffer = new byte[_controlWritePipe.PageCapacity];
            var page = await PipeFile.ReadAndReleaseAsync(_controlWritePipe, _writeSerial, buffer, CheckInterval, null);
            _writeSerial++;
            return ControlMessage.Read(buffer.AsSpan()[..(int)page.Size]);
        }
    }

    sealed class ListenerService : TunnelService
    {
        private IPEndPoint _listenAddress;

        public ListenerService(IPEndPoint listenAddress, string pipeDirectory, PipeFileConfig controlReadPipe, PipeFileConfig controlWritePipe)
            : base(pipeDirectory, controlReadPipe, controlWritePipe)
        {
            _listenAddress = listenAddress;
        }

        public void Run()
        {
            var listener = new TcpListener(_listenAddress);
            listener.Start();
            var listenerTask = listener.AcceptSocketAsync();
            
            var writerTask = WriterTask();

            while (true)
            {
                var action = Task.WaitAny(listenerTask, writerTask, Task.Delay(CheckInterval));
                switch (action)
                {
                    case 0:
                    {
                        var client = listenerTask.Result;
                        var newConnectionId = FindFreeConnection();
                        if (newConnectionId == null)
                        {
                            Console.WriteLine($"<listener> discarding connection from {client.RemoteEndPoint}");
                            client.Close();
                        }
                        else
                        {
                            SendControlMessage(ControlCommand.Connect, (byte)newConnectionId);

                            var workerReadPipe = NewWorkerReadPipe((byte)newConnectionId);
                            var workerWritePipe = NewWorkerWritePipe((byte)newConnectionId);
                            workerReadPipe.Clean();
                            workerWritePipe.Clean();
                            var context = new ConnectionContext();
                            var worker = new ConnectionWorker(client, context, workerReadPipe, workerWritePipe);
                            context.Start(worker);
                            
                            _connections[newConnectionId.Value] = context;
                            Console.WriteLine($"<listener> accepted connection from {client.RemoteEndPoint}");
                        }
                        
                        listenerTask = listener.AcceptSocketAsync();
                        break;
                    }
                    case 1:
                    {
                        var message = writerTask.Result;
                        if (message.Command == ControlCommand.Close)
                        {
                            Console.WriteLine($"<listener> peer closing {message.ConnectionId}");
                            RequestConnectionClose(message.ConnectionId, "peer closure");
                        }
                        writerTask = WriterTask();
                        break;
                    }
                }
                
                WaitForCompletedConnections();
            }
        }
    }

    sealed class RemoteService : TunnelService
    {
        private IPEndPoint _remoteAddress;

        public RemoteService(IPEndPoint remoteAddress, string pipeDirectory, PipeFileConfig controlReadPipe,
            PipeFileConfig controlWritePipe)
            : base(pipeDirectory, controlReadPipe, controlWritePipe)
        {
            _remoteAddress = remoteAddress;
        }

        public void Run()
        {
            var writerTask = WriterTask();

            while (true)
            {
                var action = Task.WaitAny(writerTask, Task.Delay(CheckInterval));
                if (action == 0)
                {
                    var message = writerTask.Result;
                    switch (message.Command)
                    {
                        case ControlCommand.Connect:
                        {
                            Console.WriteLine($"<remote> peer opened {message.ConnectionId}");
                            var client = new Socket(SocketType.Stream, ProtocolType.Tcp);
                            client.Connect(_remoteAddress);

                            var workerReadPipe = NewWorkerReadPipe(message.ConnectionId);
                            var workerWritePipe = NewWorkerWritePipe(message.ConnectionId);
                            var context = new ConnectionContext();
                            var worker = new ConnectionWorker(client, context, workerWritePipe, workerReadPipe);
                            context.Start(worker);

                            _connections[message.ConnectionId] = context;
                            Console.WriteLine($"<remote> made connection to {_remoteAddress}");
                            break;
                        }
                        case ControlCommand.Close:
                            Console.WriteLine($"<remote> peer closing {message.ConnectionId}");
                            RequestConnectionClose(message.ConnectionId, "peer closure");
                            break;
                    }

                    writerTask = WriterTask();
                }

                WaitForCompletedConnections();
            }
        }
    }

    internal class Program
    {
        private static void Usage(string context)
        {
            Console.WriteLine("Usage: filestream-proxy (listen DIRECTORY IP:PORT | forward DIRECTORY IP:PORT | clean DIRECTORY)");
            Console.WriteLine(context);
            Environment.Exit(1);
        }
        
        public static void Main(string[] args)
        {
            if (args.Length == 0)
                Usage("'listen', 'forward', or 'clean' is required");

            switch (args[0])
            {
                case "listen":
                {
                    var service = new ListenerService(IPEndPoint.Parse(args[2]),
                        args[1],
                        TunnelService.NewControlReadPipe(args[1]),
                        TunnelService.NewControlWritePipe(args[1]));
                    service.Run();
                    break;
                }
                case "forward":
                {
                    var service = new RemoteService(IPEndPoint.Parse(args[2]),
                        args[1],
                        TunnelService.NewControlWritePipe(args[1]),
                        TunnelService.NewControlReadPipe(args[1]));
                    service.Run();
                    break;
                }
                case "clean":
                {
                    var rpipe = TunnelService.NewControlReadPipe(args[1]);
                    rpipe.Clean();
                    
                    var wpipe = TunnelService.NewControlWritePipe(args[1]);
                    wpipe.Clean();
                    break;
                }
                default:
                    Usage($"Unknown command: {args[0]}");
                    break;
            }
        }
    }
}
