﻿using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;

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
        public static async Task SendAll(this Socket socket, ReadOnlyMemory<byte> memory, CancellationToken cancel)
        {
            var offset = 0;
            var toSend = memory.Length;
            while (toSend > 0)
            {
                if (cancel.IsCancellationRequested)
                    throw new TaskCanceledException();
                
                var sent = await socket.SendAsync(memory.Slice(offset, toSend), SocketFlags.None, cancel);
                if (sent == 0)
                    throw new IOException();
                
                offset += sent;
                toSend -= sent;
            }
        }
    }

    /// Base class for services that manage multiple connections.
    abstract class TunnelService
    {
        private const int MaxConnections = byte.MaxValue;
        private const int ControlPageCount = MaxConnections;
        private const int ControlPageSize = ControlMessage.TotalSize;
        private const int WorkerPageCount = 64;
        private const int WorkerPageSize = 8192;

        private string _pipeDirectory;
        private ControlPipe _controlPipe;
        
        private readonly ReadWriteConnection?[] _connections = new ReadWriteConnection?[MaxConnections];
        private IList<Task> _connectionTasks;

        protected ILogger Logger;
        
        protected TunnelService(string pipeDirectory, PipeFileConfig controlReadPipe, PipeFileConfig controlWritePipe)
        {
            _pipeDirectory = pipeDirectory;
            _controlPipe = new ControlPipe(controlReadPipe, controlWritePipe);
            _connectionTasks = ReadWriteConnection.InitTasks(MaxConnections);
            
            Logger = Program.LogFactory.CreateLogger(GetType());
        }

        /// Reads a control message from the pipe.
        protected Task<ControlMessage> ReadCommand() => _controlPipe.Receive();

        /// Gets all the connection tasks.
        protected IEnumerable<Task> ConnectionTasks() => _connectionTasks;
        
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
        
        /// Registers a connection from a bound socket. Returns true if there was a free slot for the connection,
        /// or false if there was none.
        protected async Task<bool> AddServerConnection(Socket socket)
        {
            var cid = FindFreeConnection();
            if (cid == null)
            {
                Logger.LogDebug("No room for new connection");
                return false;
            }
            var connectionId = cid.Value;

            var readPipe = NewWorkerReadPipe(connectionId);
            readPipe.Clean();
            var writePipe = NewWorkerWritePipe(connectionId);
            writePipe.Clean();

            var cnx = ReadWriteConnection.Create(connectionId, socket, readPipe, writePipe);
            _connections[connectionId] = cnx;
            cnx.AddTasks(_connectionTasks);

            await _controlPipe.Send(ControlCommand.Connect, connectionId);
            Logger.LogDebug("Allocated new connection {0}", connectionId);
            return true;
        }
        
        /// Registers a connection after the peer accepted it.
        protected void AddClientConnection(byte connectionId, Socket socket)
        {
            Debug.Assert(_connections[connectionId] == null, "Connection from server conflicts with local connection");
            var readPipe = NewWorkerReadPipe(connectionId);
            var writePipe = NewWorkerWritePipe(connectionId);
            var cnx = ReadWriteConnection.Create(connectionId, socket, writePipe, readPipe);
            _connections[connectionId] = cnx;
            cnx.AddTasks(_connectionTasks);
            Logger.LogDebug("Linked new connection {0}", connectionId);
        }

        /// Processes a close message for the given connection.
        protected void ProcessCancel(byte connectionId, ConnectionDirection direction)
        {
            var cnx = _connections[connectionId];
            Debug.Assert(cnx != null, "Unable to process close on null connection");
            cnx.ProcessCancel(direction);
            if (cnx.ReadyToClose)
                RemoveConnection(cnx);
            else
                Logger.LogDebug("{0} {1} zombie", connectionId, direction);
        }
        
        /// Processes a close message for the given connection.
        protected Task ConnectionDone(Task task)
        {
            var worker = (task as Task<ConnectionWorker>).Result;
            worker.RemoveTask(_connectionTasks);
            if (worker.Owner.ReadyToClose)
                RemoveConnection(worker.Owner);
            return worker.SendCloseNotification(_controlPipe);
        }

        /// Removes a dead connection from the connection table.
        private void RemoveConnection(ReadWriteConnection connection)
        {
            Logger.LogInformation("{0} finished", connection.Id);
            connection.Close();
            _connections[connection.Id] = null;
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
                ReadCommand()
            };
            
            while (true)
            {
                var tasks = ConnectionTasks().Concat(coreTasks);
                var nextTask = await Task.WhenAny(tasks);
                if (ReferenceEquals(nextTask, coreTasks[LISTENER_TASK]))
                {
                    var client = (nextTask as Task<Socket>).Result;
                    if (!await AddServerConnection(client))
                        client.Close();
                    
                    coreTasks[LISTENER_TASK] = listener.AcceptSocketAsync();
                }
                else if (ReferenceEquals(nextTask, coreTasks[WRITER_TASK]))
                {
                    var message = (nextTask as Task<ControlMessage>).Result;
                    if (message.Command == ControlCommand.CloseRead)
                        ProcessCancel(message.ConnectionId, ConnectionDirection.Reader);
                    else if (message.Command == ControlCommand.CloseWrite)
                        ProcessCancel(message.ConnectionId, ConnectionDirection.Writer);
                    
                    coreTasks[WRITER_TASK] = ReadCommand();
                }
                else
                    await ConnectionDone(nextTask);
            }
        }
    }

    sealed class RemoteService : TunnelService
    {
        private IPEndPoint _remoteAddress;

        public RemoteService(IPEndPoint remoteAddress, string pipeDirectory, PipeFileConfig controlReadPipe, PipeFileConfig controlWritePipe)
            : base(pipeDirectory, controlReadPipe, controlWritePipe)
        {
            _remoteAddress = remoteAddress;
        }

        public async Task Run()
        {
            var coreTasks = new Task[] { ReadCommand() };
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
                            await client.ConnectAsync(_remoteAddress);
                            AddClientConnection(message.ConnectionId, client);
                            break;
                        }
                        case ControlCommand.CloseRead:
                            ProcessCancel(message.ConnectionId, ConnectionDirection.Reader);
                            break;
                        case ControlCommand.CloseWrite:
                            ProcessCancel(message.ConnectionId, ConnectionDirection.Writer);
                            break;
                    }
                    coreTasks[0] = ReadCommand();
                }
                else
                    await ConnectionDone(nextTask);
            }
        }
    }

    internal class Program
    {
        public static readonly ILoggerFactory LogFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.SetMinimumLevel(LogLevel.Information);
        });
        
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