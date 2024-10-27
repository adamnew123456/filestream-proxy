using System.Net;
using System.Net.Sockets;

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
        public static void SendAll(this Socket socket, ReadOnlySpan<byte> span, CancellationToken cancel)
        {
            var offset = 0;
            var toSend = span.Length;
            while (toSend > 0)
            {
                if (cancel.IsCancellationRequested)
                    throw new OperationCanceledException();
                    
                var sent = socket.Send(span.Slice(offset, toSend));
                offset += sent;
                toSend -= sent;
            }
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
        /// Identifier for the connection. Unique among live connections, but may be recycled
        /// after the current connection closes.
        public byte Id { get; }

        /// Cancellation used to stop the connection worker.
        public CancellationToken Token { get; }

        /// Request that the connection worker stops. May be called from inside the worker.
        public void Cancel(bool notifyPeer, string reason);
    }

    /// Passes data between a socket and a read/write pipe file.
    sealed class ConnectionWorker
    {
        private static readonly TimeSpan CheckInterval = TimeSpan.FromMilliseconds(50);
        
        private Socket _socket;
        private IConnectionMaster _master;
        private PipeFileConfig _readPipe;
        private PipeFileConfig _writePipe;
        private long _bytesRead;
        private long _bytesWritten;

        public ConnectionWorker(Socket socket, IConnectionMaster master, PipeFileConfig readPipe, PipeFileConfig writePipe)
        {
            _socket = socket;
            _master = master;
            _readPipe = readPipe;
            _writePipe = writePipe;
        }

        /// Passes data from the socket to the read pipe, and from the write pipe onto the socket. Stops when the cancellation trips or an exception occurs.
        public async Task Run()
        {
            ulong readSerial = PageHeader.SerialUnused + 1;
            var readBuffer = new byte[_readPipe.PageCapacity];
            var readerTask = ReaderTask(readSerial, readBuffer);

            ulong writeSerial = PageHeader.SerialUnused + 1;
            var writeBuffer = new byte[_readPipe.PageCapacity];
            var writerTask = WriterTask(writeSerial, writeBuffer);

            while (!_master.Token.IsCancellationRequested)
            {
                try
                {
                    var nextTask = await Task.WhenAny(readerTask, writerTask);
                    if (ReferenceEquals(nextTask, readerTask))
                    {
                        readSerial = readerTask.Result;
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
                    _master.Cancel(true, $"connection exception ({err.Message})");
                }
            }

            _socket.Close();
            _socket.Dispose();
        }

        /// Receives data from the socket and passes it into the read pipe.
        private async Task<ulong> ReaderTask(ulong serial, byte[] buffer)
        {
            var read = await _socket.ReceiveAsync(buffer, SocketFlags.None, _master.Token);
            if (read == 0)
            {
                // Avoid spinning if the socket isn't doing any IO. Also check that the socket is
                // still live - a zero-byte read could indicate closure.
                await _socket.SendAsync(new ReadOnlyMemory<byte>(buffer, 0, 0), SocketFlags.None, _master.Token);
                await Task.Delay(CheckInterval);
                return serial;
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
            _socket.SendAll(buffer.AsSpan()[..(int)page.Size], _master.Token);
            return serial + 1;
        }
    }

    /// Tracks a connection worker along with its cancellation and status.
    sealed class ConnectionContext : IConnectionMaster, IDisposable
    {
        private CancellationTokenSource _source;
        
        public byte Id { get; }
        public CancellationToken Token { get; }
        public Task Worker { get; set; }
        
        /// Whether to notify the other end of the control pipe. Not required when the peer requested the close, but
        /// will be true if the socket closed on our end.
        public bool NotifyPeer { get; private set; }

        public ConnectionContext(byte id)
        {
            _source = new CancellationTokenSource();
            Id = id;
            Token = _source.Token;
        }

        /// Asks the worker to stop itself.
        public void Cancel(bool notifyPeer, string reason)
        {
            NotifyPeer = notifyPeer;
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
        protected static readonly TimeSpan ConnectionReapInterval = TimeSpan.FromSeconds(1);
        private const int MaxConnections = byte.MaxValue;
        private const int ControlPageCount = MaxConnections;
        private const int ControlPageSize = ControlMessage.TotalSize;
        private const int WorkerPageCount = 64;
        private const int WorkerPageSize = 8192;

        private string _pipeDirectory;
        private PipeFileConfig _controlReadPipe;
        private PipeFileConfig _controlWritePipe;

        private ulong _readSerial = PageHeader.SerialUnused + 1;
        private ulong _writeSerial = PageHeader.SerialUnused + 1;
        protected readonly ConnectionContext?[] _connections = new ConnectionContext?[MaxConnections];
        
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
        protected void RequestConnectionClose(byte connectionId)
        {
            var cnx = _connections[connectionId];
            if (cnx == null) return;
            cnx.Cancel(true, "requested by peer");
        }
        
        /// Checks all valid connections and closes any whose cancellations have triggered.
        protected void WaitForCompletedConnections()
        {
            for (byte i = 0; i < _connections.Length; i++)
            {
                var cnx = _connections[i];
                if (cnx == null) continue;
                if (cnx.Worker.IsCompleted)
                {
                    if (cnx.NotifyPeer)
                        SendControlMessage(ControlCommand.Close, i);
                    
                    _connections[i].Dispose();
                    _connections[i] = null;
                }
            }
        }

        /// Gets an enumerable containing tasks for all live connections.
        protected IEnumerable<Task> ConnectionTasks()
        {
            return _connections
                .Where(c => c != null)
                .Select(c => c.Worker);
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

        public async Task Run()
        {
            const int LISTENER_TASK = 0;
            const int WRITER_TASK = 1;
            
            var listener = new TcpListener(_listenAddress);
            listener.Start();
            var coreTasks = new Task[]
            {
                listener.AcceptSocketAsync(),
                WriterTask()
            };
            
            while (true)
            {
                var tasks = ConnectionTasks().Concat(coreTasks);
                var nextTask = await Task.WhenAny(tasks);
                if (ReferenceEquals(nextTask, coreTasks[LISTENER_TASK]))
                {
                    var client = (nextTask as Task<Socket>).Result;
                    var newConnectionId = FindFreeConnection();
                    if (newConnectionId == null)
                    {
                        client.Close();
                    }
                    else
                    {
                        SendControlMessage(ControlCommand.Connect, (byte)newConnectionId);

                        var workerReadPipe = NewWorkerReadPipe((byte)newConnectionId);
                        var workerWritePipe = NewWorkerWritePipe((byte)newConnectionId);
                        workerReadPipe.Clean();
                        workerWritePipe.Clean();
                        var context = new ConnectionContext((byte)newConnectionId);
                        var worker = new ConnectionWorker(client, context, workerReadPipe, workerWritePipe);
                        context.Worker = worker.Run();
                        
                        _connections[newConnectionId.Value] = context;
                    }
                    
                    coreTasks[LISTENER_TASK] = listener.AcceptSocketAsync();
                }
                else if (ReferenceEquals(nextTask, coreTasks[WRITER_TASK]))
                {
                    var message = (nextTask as Task<ControlMessage>).Result;
                    if (message.Command == ControlCommand.Close)
                    {
                        RequestConnectionClose(message.ConnectionId);
                    }
                    coreTasks[WRITER_TASK] = WriterTask();
                }
                else
                {
                    WaitForCompletedConnections();
                }
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

        public async Task Run()
        {
            var coreTasks = new Task[] { WriterTask() };
            while (true)
            {
                var tasks = ConnectionTasks().Concat(coreTasks);
                var nextTask = await Task.WhenAny(tasks);
                if (ReferenceEquals(nextTask, coreTasks[0]))
                {
                    var message = (nextTask as Task<ControlMessage>).Result;
                    switch (message.Command)
                    {
                        case ControlCommand.Connect:
                        {
                            var client = new Socket(SocketType.Stream, ProtocolType.Tcp);
                            client.Connect(_remoteAddress);

                            var workerReadPipe = NewWorkerReadPipe(message.ConnectionId);
                            var workerWritePipe = NewWorkerWritePipe(message.ConnectionId);
                            var context = new ConnectionContext(message.ConnectionId);
                            var worker = new ConnectionWorker(client, context, workerWritePipe, workerReadPipe);
                            context.Worker = worker.Run();

                            _connections[message.ConnectionId] = context;
                            break;
                        }
                        case ControlCommand.Close:
                            RequestConnectionClose(message.ConnectionId);
                            break;
                    }

                    coreTasks[0] = WriterTask();
                }
                else
                {
                    WaitForCompletedConnections();
                }
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
        
        public static async Task Main(string[] args)
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
                    await service.Run();
                    break;
                }
                case "forward":
                {
                    var service = new RemoteService(IPEndPoint.Parse(args[2]),
                        args[1],
                        TunnelService.NewControlWritePipe(args[1]),
                        TunnelService.NewControlReadPipe(args[1]));
                    await service.Run();
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