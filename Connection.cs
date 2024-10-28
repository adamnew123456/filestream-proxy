using System.Diagnostics;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;

namespace filestream_proxy;

/// A specific flow within a TCP connection. Read and write are independent, we can have the read
/// end closed while writing or vice versa. The connection can only be cleaned up when both ends
/// have closed.
enum ConnectionDirection
{
    Writer,
    Reader,
}

/// What a connection is currently doing.
enum ConnectionState
{
    /// The initial state of the connection. The connection moves from this state to Running when
    /// the worker is awaited the first time.
    Unstarted,
    
    /// The connection is sending data. The connection manager must await the connection until it
    /// changes state. The next state is either Cancelling (if we get a cancel message from the peer)
    /// or PendingCloseConfirm (if we detect a close and have to tell our peer)
    Running,
    
    /// A cancel has been requested on the connection. The connection manager must still await the connection
    /// until the cancellation is processed. The next state is Closed.
    Cancelling,
    
    /// The connection has closed on our end. The connection manager must notify us when it receives a cancel.
    PendingCloseConfirm,
    
    /// The connection has closed and been acknowledged. The connection manager must recycle the connection slot.
    Closed
}

/// A connection worker manages the direction-independent parts of a connection.
abstract class ConnectionWorker
{
    protected readonly TimeSpan CheckInterval = TimeSpan.FromMilliseconds(50);
    protected readonly TimeSpan SocketTimeout = TimeSpan.FromMinutes(1);
    
    protected Socket Socket;
    protected PipeFileConfig DataPipe;
    protected byte[] Buffer;
    protected CancellationToken Cancel;
    protected ILogger Logger;
    protected long Bytes;
    protected abstract ConnectionDirection Direction { get; }

    private byte _connectionId;
    private Task? _task;
    private CancellationTokenSource _cancelSource;
    
    public ReadWriteConnection Owner { get; set; }
    /// What the connection is currently doing. See the documentation for the ConnectionState members for more information.
    public ConnectionState State { get; private set; }

    protected ConnectionWorker(byte connectionId, Socket socket, PipeFileConfig dataPipe)
    {
        _cancelSource = new CancellationTokenSource();
        _connectionId = connectionId;
        Logger = Program.LogFactory.CreateLogger($"{GetType()}.{_connectionId}");
        Socket = socket;
        DataPipe = dataPipe;
        Cancel = _cancelSource.Token;
        State = ConnectionState.Unstarted;
        Buffer = new byte[dataPipe.PageCapacity];
    }
    
    /// Notifies the peer that this half of the connection has closed.
    public Task SendCloseNotification(ControlPipe pipe)
    {
        Logger.LogDebug("Notify close");
        var peerDirection = Direction == ConnectionDirection.Reader ? ControlCommand.CloseWrite : ControlCommand.CloseRead;
        return pipe.Send(peerDirection, _connectionId);
    }
    
    /// Updates the task list and associated data mapping to include this connection.
    public void RemoveTask(IList<Task> tasks)
    {
        var idx = _connectionId * 2;
        if (Direction == ConnectionDirection.Writer) idx++;
        tasks[idx] = Task.Delay(-1);
    }
    
    /// Waits for the connection to finish processing data. Once this task returns, the connection is in one
    /// of the non-running states. If the connection is already in a non-running state, this returns a task
    /// that blocks forever.
    public Task WaitForClose()
    {
        switch (State)
        {
            case ConnectionState.Unstarted:
                Logger.LogDebug("Starting IO task");
                State = ConnectionState.Running;
                _task = Worker();
                return _task;
            case ConnectionState.Running:
            case ConnectionState.Cancelling:
                Debug.Assert(_task != null, "Running connection worker must have task");
                return _task;
            default:
                return Task.Delay(-1);
        }
    }

    /// Process a cancellation message from the peer.
    public void ProcessCancel()
    {
        if (State == ConnectionState.Running)
        {
            Logger.LogDebug("Processing cancel on live connection");
            State = ConnectionState.Cancelling;
            _cancelSource.Cancel();
        }
        else if (State == ConnectionState.PendingCloseConfirm)
        {
            Logger.LogDebug("Processing cancel on zombie connection");
            State = ConnectionState.Closed;
        }
    }

    /// Wraps the read-specific/write-specific task which updates the connection state and cleans up resources.
    private async Task<ConnectionWorker> Worker()
    {
        Logger.LogDebug("Beginning IO loop");
        try
        {
            await InnerWorker();
        }
        catch (Exception ex)
        {
            if (ex is AggregateException)
                ex = ((AggregateException)ex).InnerExceptions[0];
            
            Logger.LogDebug("IO exception: {0}", ex.Message);
        }
        finally
        {
            // Next state depends on the reason for the close; if we closed because of the peer then we don't have to
            // wait for a confirmation
            if (State == ConnectionState.Running)
            {
                Logger.LogInformation("Stopped by IO after {0} bytes, waiting for cancel", Bytes);
                State = ConnectionState.PendingCloseConfirm;
            }
            else
            {
                Logger.LogInformation("Stopped by cancel after {0} bytes, closing", Bytes);
                State = ConnectionState.Closed;
            }

            Socket.Shutdown(Direction == ConnectionDirection.Reader ? SocketShutdown.Receive : SocketShutdown.Send);
            _cancelSource.Dispose();
            _task = null;
        }
        return this;
    }
    
    /// Transfers data from the socket to the pipe file, or vice versa.
    protected abstract Task InnerWorker();
}

/// Connection worker that moves data from the socket to the pipe file.
class ReadWorker : ConnectionWorker
{
    protected override ConnectionDirection Direction => ConnectionDirection.Reader;

    public ReadWorker(byte connectionId, Socket socket, PipeFileConfig dataPipe) : base(connectionId, socket, dataPipe)
    {
        socket.ReceiveTimeout = (int)SocketTimeout.TotalMilliseconds;
    }

    protected override async Task InnerWorker()
    {
        Logger.LogDebug("Transfer socket -> {0}", DataPipe.PipePath);
        var serial = PageHeader.SerialUnused + 1;
        while (true)
        {
            Logger.LogTrace("Wait for recv");
            var read = await Socket.ReceiveAsync(Buffer, SocketFlags.None, Cancel);
            if (read == 0) return;

            Logger.LogTrace("Wait for page {0}", read);
            Bytes += read;
            await PipeFile.AllocateAndWriteAsync(DataPipe, serial, Buffer.AsMemory(0, read), CheckInterval, Cancel);
            serial++;
        }
    }
}

/// Connection worker that moves data from the pipe file to the socket.
class WriteWorker : ConnectionWorker
{
    protected override ConnectionDirection Direction => ConnectionDirection.Writer;

    public WriteWorker(byte connectionId, Socket socket, PipeFileConfig dataPipe) : base(connectionId, socket, dataPipe)
    {
        socket.SendTimeout = (int)SocketTimeout.TotalMilliseconds;
    }

    protected override async Task InnerWorker()
    {
        Logger.LogDebug("Transfer socket <- {0}", DataPipe.PipePath);
        var serial = PageHeader.SerialUnused + 1;
        while (true)
        {
            Logger.LogTrace("Wait for page");
            var page = await PipeFile.ReadAndReleaseAsync(DataPipe, serial, Buffer, CheckInterval, Cancel);
            Logger.LogTrace("Wait for send {0}", page.Size);
            await Socket.SendAll(Buffer.AsMemory(0, (int)page.Size), Cancel);
            Bytes += page.Size;
            serial++;
        }
    }
}

/// Wrapper around a reader/writer pair which manages pending task list.
sealed class ReadWriteConnection
{
    private byte _connectionId;
    private Socket _socket;
    private ReadWorker _reader;
    private WriteWorker _writer;
    
    public byte Id { get; }
    
    /// Whether both ends of the connections have closed and received confirmation from the peer.
    public bool ReadyToClose => _reader.State == ConnectionState.Closed && _writer.State == ConnectionState.Closed;

    private ReadWriteConnection(byte connectionId, Socket socket, ReadWorker reader, WriteWorker writer)
    {
        _connectionId = connectionId;
        _socket = socket;
        _reader = reader;
        _writer = writer;
        _reader.Owner = this;
        _writer.Owner = this;
        Id = connectionId;
    }

    /// Processes a cancel message for one direction of the connection.
    public void ProcessCancel(ConnectionDirection direction)
    {
        if (direction == ConnectionDirection.Reader)
            _reader.ProcessCancel();
        else
            _writer.ProcessCancel();
    }
    

    /// Updates the task list and associated data mapping to include this connection.
    public void AddTasks(IList<Task> tasks)
    {
        var idx = _connectionId * 2;
        tasks[idx] = _reader.WaitForClose();
        tasks[idx + 1] = _writer.WaitForClose();
    }
    
    /// Closes the socket associated with this connection.
    public void Close()
    {
        _socket.Close();
    }

    public static ReadWriteConnection Create(byte connectionId, Socket socket, PipeFileConfig readPipe, PipeFileConfig writePipe)
    {
        var reader = new ReadWorker(connectionId, socket, readPipe);
        var writer = new WriteWorker(connectionId, socket, writePipe);
        return new ReadWriteConnection(connectionId, socket, reader, writer);
    }

    /// Creates a task list containing no pending connection tasks. Pass this into the AddTasks and RemoveTasks methods when a connection starts/stops.
    public static IList<Task> InitTasks(int maxConnections)
    {
        var tasks = new List<Task>();
        for (int i = 0; i < maxConnections; i++)
        {
            tasks.Add(Task.Delay(Timeout.Infinite));
            tasks.Add(Task.Delay(Timeout.Infinite));
        }
        return tasks;
    }
}