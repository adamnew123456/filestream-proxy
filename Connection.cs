using System.Diagnostics;
using System.Net.Sockets;

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
abstract class BaseConnectionWorker
{
    protected readonly TimeSpan CheckInterval = TimeSpan.FromMilliseconds(50);
    protected readonly TimeSpan SocketTimeout = TimeSpan.FromMinutes(1);
    
    protected Socket Socket;
    protected PipeFileConfig DataPipe;
    protected byte[] Buffer;
    protected CancellationToken Cancel;
    protected abstract ConnectionDirection Direction { get; }
    
    private byte _connectionId;
    private Task? _task;
    private CancellationTokenSource _cancelSource;
    private ControlPipe _controlPipe;
    
    /// What the connection is currently doing. See the documentation for the ConnectionState members for more information.
    public ConnectionState State { get; private set; }

    protected BaseConnectionWorker(byte connectionId, Socket socket, PipeFileConfig dataPipe, ControlPipe controlPipe)
    {
        _cancelSource = new CancellationTokenSource();
        _connectionId = connectionId;
        _controlPipe = controlPipe;
        Socket = socket;
        DataPipe = dataPipe;
        Cancel = _cancelSource.Token;
        State = ConnectionState.Unstarted;
        Buffer = new byte[dataPipe.PageCapacity];
    }

    /// Waits for the connection to finish processing data. Once this task returns, the connection is in one
    /// of the non-running states. If the connection is already in a non-running state, this returns a task
    /// that blocks forever.
    public Task WaitForClose()
    {
        switch (State)
        {
            case ConnectionState.Unstarted:
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
            State = ConnectionState.Cancelling;
            _cancelSource.Cancel();
        }
        else if (State == ConnectionState.PendingCloseConfirm)
            State = ConnectionState.Closed;
    }

    /// Wraps the read-specific/write-specific task which updates the connection state and cleans up resources.
    private async Task Worker()
    {
        try
        {
            await InnerWorker();
        }
        catch (Exception)
        {
            // Just close the connection in response to an IO error or cancellation, the error is unimportant
        }
        finally
        {
            // Next state depends on the reason for the close; if we closed because of the peer then we don't have to
            // wait for a confirmation
            if (State == ConnectionState.Running)
            {
                State = ConnectionState.PendingCloseConfirm;
                // Must be reversed since our writer corresponds to the peer's reader, and vice versa
                var command = Direction == ConnectionDirection.Reader ? ControlCommand.CloseWrite : ControlCommand.CloseRead;
                await _controlPipe.Send(command, _connectionId);
            }
            else
                State = ConnectionState.Closed;
            
            Socket.Shutdown(Direction == ConnectionDirection.Reader ? SocketShutdown.Receive : SocketShutdown.Send);
            _cancelSource.Dispose();
            _task = null;
        }
    }
    
    /// Transfers data from the socket to the pipe file, or vice versa.
    protected abstract Task InnerWorker();
}

/// Connection worker that moves data from the socket to the pipe file.
class ReadWorker : BaseConnectionWorker
{
    protected override ConnectionDirection Direction => ConnectionDirection.Reader;

    public ReadWorker(byte connectionId, Socket socket, PipeFileConfig dataPipe, ControlPipe controlPipe) : base(connectionId, socket, dataPipe, controlPipe)
    {
        socket.ReceiveTimeout = (int)SocketTimeout.TotalMilliseconds;
    }

    protected override async Task InnerWorker()
    {
        var serial = PageHeader.SerialUnused + 1;
        while (true)
        {
            var read = await Socket.ReceiveAsync(Buffer, SocketFlags.None, Cancel);
            if (read == 0) return;

            await PipeFile.AllocateAndWriteAsync(DataPipe, serial, Buffer.AsMemory(0, read), CheckInterval, Cancel);
            serial++;
        }
    }
}

/// Connection worker that moves data from the pipe file to the socket.
class WriteWorker : BaseConnectionWorker
{
    protected override ConnectionDirection Direction => ConnectionDirection.Writer;

    public WriteWorker(byte connectionId, Socket socket, PipeFileConfig dataPipe, ControlPipe controlPipe) : base(connectionId, socket, dataPipe, controlPipe)
    {
        socket.SendTimeout = (int)SocketTimeout.TotalMilliseconds;
    }

    protected override async Task InnerWorker()
    {
        var serial = PageHeader.SerialUnused + 1;
        while (true)
        {
            var page = await PipeFile.ReadAndReleaseAsync(DataPipe, serial, Buffer, CheckInterval, Cancel);
            await Socket.SendAll(Buffer.AsMemory(0, (int)page.Size), Cancel);
            serial++;
        }
    }
}

/// Holds data associated with a reader or writer task.
struct ConnectionKey
{
    public ReadWriteConnection Connection { get; init; }
    public ConnectionDirection Direction { get; init; }
}

/// Wrapper around a reader/writer pair which manages pending task list.
sealed class ReadWriteConnection
{
    private byte _connectionId;
    private Socket _socket;
    private ReadWorker _reader;
    private WriteWorker _writer;
    
    /// Whether both ends of the connections have closed and received confirmation from the peer.
    public bool ReadyToClose => _reader.State == ConnectionState.Closed && _writer.State == ConnectionState.Closed;

    private ReadWriteConnection(byte connectionId, Socket socket, ReadWorker reader, WriteWorker writer)
    {
        _connectionId = connectionId;
        _socket = socket;
        _reader = reader;
        _writer = writer;
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
    public void AddTasks(IList<Task> tasks, IDictionary<Task, ConnectionKey> owners)
    {
        var readerTask = _reader.WaitForClose();
        tasks[_connectionId * 2] = readerTask;
        owners[readerTask] = new ConnectionKey { Connection = this, Direction = ConnectionDirection.Reader };
        
        var writerTask = _writer.WaitForClose();
        tasks[(_connectionId * 2) + 1] = writerTask;
        owners[writerTask] = new ConnectionKey { Connection = this, Direction = ConnectionDirection.Writer };
    }
    
    /// Updates the task list and associated data mapping to exclude this connection.
    public void RemoveTasks(IList<Task> tasks, IDictionary<Task, ConnectionKey> owners)
    {
        var readerTask = tasks[_connectionId * 2];
        tasks[_connectionId * 2] = Task.Delay(Timeout.Infinite);
        owners.Remove(readerTask);
        
        var writerTask = tasks[(_connectionId * 2) + 1];
        tasks[(_connectionId * 2) + 1] = Task.Delay(Timeout.Infinite);
        owners.Remove(writerTask);

        _socket.Close();
    }

    public static ReadWriteConnection Create(byte connectionId, Socket socket, PipeFileConfig readPipe, PipeFileConfig writePipe, ControlPipe controlPipe)
    {
        var reader = new ReadWorker(connectionId, socket, readPipe, controlPipe);
        var writer = new WriteWorker(connectionId, socket, writePipe, controlPipe);
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