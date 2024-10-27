namespace filestream_proxy;

/// Commands that can be sent over the command pipe.
enum ControlCommand : byte
{
    /// Should not be used, only visible when a page has not been written to.
    None = 0,
    /// Instructs the client end to connect to the remote, and assign the connection the ID
    /// in the page header. Any data in the page is discarded.
    Connect,
    /// Instructs the peer (client or server can send this) to close the read end of its
    /// connection.
    CloseRead,
    /// Instructs the peer (client or server can send this) to close the write end of its
    /// connection.
    CloseWrite,
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

/// Sends and receives connection state notifications from the peer.
sealed class ControlPipe
{
    private readonly TimeSpan CheckInterval = TimeSpan.FromMilliseconds(50);
    
    private ulong _readSerial;
    private ulong _writeSerial;
    private byte[] _readBuffer;
    private byte[] _writeBuffer;
    private PipeFileConfig _readPipe;
    private PipeFileConfig _writePipe;

    public ControlPipe(PipeFileConfig readPipe, PipeFileConfig writePipe)
    {
        _readPipe = readPipe;
        _writePipe = writePipe;
        _readSerial = PageHeader.SerialUnused + 1;
        _writeSerial = PageHeader.SerialUnused + 1;
        _readBuffer = new byte[_readPipe.PageCapacity];
        _writeBuffer = new byte[_readPipe.PageCapacity];
    }
    
    /// Send a control message over the tunnel's control pipe.
    public async Task Send(ControlCommand command, byte connectionId)
    {
        var message = new ControlMessage
        {
            Command = command,
            ConnectionId = connectionId
        };

        message.Write(_writeBuffer);
        await PipeFile.AllocateAndWriteAsync(_writePipe, _writeSerial, _writeBuffer, CheckInterval, null); 
        _writeSerial++;
    }
    
    /// Reads a control message from the control pipe.
    public async Task<ControlMessage> Receive()
    {
        var page = await PipeFile.ReadAndReleaseAsync(_readPipe, _readSerial, _readBuffer, CheckInterval, null);
        _readSerial++;
        return ControlMessage.Read(_readBuffer.AsSpan()[..(int)page.Size]);
    }
}