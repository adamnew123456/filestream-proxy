# What is this?

This is a file-based TCP proxy. Create a directory for the proxy to hold its
buffers, then start a listener and a forwarder. The listener accepts a TCP
connection and the forwarder passes its data to some other host:

```bash
$ mkdir /tmp/fs-proxy
$ dotnet run listen /tmp/fs-proxy 127.0.0.1:8000 &
$ dotnet run forward /tmp/fs-proxy 1.1.1.1:80 &
$ curl -H 'Host: 1.1.1.1' http://localhost:8000
```

(HTTP isn't the best example here because of vhosting, use your imagination.
You can run SSH or anything else over this thing if you want.)

# What does it do?

The core of this tool is the *pipe file*, which supports ordered communication
between two processes using a pair of shared files, the pipe data file and the
pipe lock file. Note that a pipe file is unidirectional - you need a pair of
pipe files to get full duplex communication.

At the start, both the listener and the forwarder create a command pipe. This
pipe carries notifications about connection data - opens and closes. When the
listener gets a connection it configures a separate pipe for that connection,
notifies the forwarder, and starts a connection worker that passes data between
the socket and the pipe. The forwarder then creates a connection to the remote
address and also runs a connection worker.

# Eck! Why?

To have a convenient reverse tunnel that I can run over RDP. If I share a
folder with the server, I can run this program on the server and on the client
and get a functional TCP connection from the server to a port on my machine.

# Is it fast?

No. In the process of adding multi-connection support, something (I suspect
lock management) has gotten noticeably worse. It's still fast enough for SSH
though.
