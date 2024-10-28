using System.Buffers;
using System.Collections;
using System.Text;
using Microsoft.Extensions.Logging;

namespace filestream_proxy;

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
    
    /// The logger to write messages about this pipe to.
    private ILogger? Logger;
        
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

    public ILogger GetLogger()
    {
        if (Logger == null)
            Logger = Program.LogFactory.CreateLogger($"PipeFile.{PipePath}");
        return Logger;
    }

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
    public static async Task AllocateAndWriteAsync(PipeFileConfig config, ulong serial, ReadOnlyMemory<byte> buffer, TimeSpan checkInterval, CancellationToken? cancel)
    {
        if (buffer.Length > config.PageCapacity)
            throw new ArgumentException($"Buffer capacity too large: {buffer.Length} > {config.PageCapacity}");
            
        var header = new PageHeader
        {
            Serial = serial,
            Size = (uint) buffer.Length
        };

        var failedLocks = 0;
        var failedPages = 0;
        var delay = false;
        while (true)
        {
            if (cancel != null && cancel.Value.IsCancellationRequested)
                throw new TaskCanceledException();

            if (delay)
                await Task.Delay(checkInterval);
            delay = true;

            var lockFile = LockFile.Acquire(config.LockPath);
            if (lockFile == null)
            {
                failedLocks++;
                continue;
            }

            using var pipeFile = new PipeFile(lockFile, config);
            if (!pipeFile.AllocatePage(header))
            {
                failedPages++;
                continue;
            }
            pipeFile.WritePage(serial, buffer.Span);
            config.GetLogger().LogTrace("Write completed {0}, locks:{1}, pages:{2}", serial, failedLocks, failedPages);
            return;
        }
    }
        
    /// Finds the page in the pipe for the provided serial and reads it into the given buffer.
    /// If no page representing that serial is present, this sleeps for the check interval
    /// and retries until the page is found.  
    public static async Task<PageHeader> ReadAndReleaseAsync(PipeFileConfig config, ulong serial, Memory<byte> buffer, TimeSpan checkInterval, CancellationToken? cancel)
    {
        if (buffer.Length < config.PageCapacity)
            throw new ArgumentException($"Buffer capacity to small: {buffer.Length} < {config.PageCapacity}");
            
        var failedLocks = 0;
        var failedPages = 0;
        var delay = false;
        while (true)
        {
            if (cancel != null && cancel.Value.IsCancellationRequested)
                throw new TaskCanceledException();

            if (delay)
                await Task.Delay(checkInterval);
            delay = true;

            var lockFile = LockFile.Acquire(config.LockPath);
            if (lockFile == null)
            {
                failedLocks++;
                continue;
            }

            using var pipeFile = new PipeFile(lockFile, config);
            if (!pipeFile.ReadPage(serial, buffer.Span, out var header))
            {
                failedPages++;
                continue;
            }

            pipeFile.ReleasePage(serial);
            config.GetLogger().LogTrace("Read completed {0}, locks:{1}, pages:{2}", serial, failedLocks, failedPages);
            return header;
        }
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