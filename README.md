# What is this?

This is a file-based TCP proxy. Create a directory for the proxy to hold its
buffers, then start a listener and a forwarder. The listener accepts a TCP
connection and the forwarder passes its data to some other host:

```bash
$ mkdir /tmp/fs-proxy
$ dotnet clean /tmp/fs-proxy # Does nothing here, but recommended when doing multiple runs
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

It doesn't compare to a real network connection, but in ideal conditions (where
the stream directory is on a tmpfs) you can get several megabytes per second!

```
$ iperf3 -c localhost -p 9090
Connecting to host localhost, port 9090
[  5] local 127.0.0.1 port 36716 connected to 127.0.0.1 port 9090
[ ID] Interval           Transfer     Bitrate         Retr  Cwnd
[  5]   0.00-1.00   sec  16.0 MBytes   134 Mbits/sec    0   3.25 MBytes       
[  5]   1.00-2.00   sec  9.00 MBytes  75.5 Mbits/sec    0   3.25 MBytes       
[  5]   2.00-3.00   sec  9.50 MBytes  79.7 Mbits/sec    0   3.25 MBytes       
[  5]   3.00-4.00   sec  9.50 MBytes  79.7 Mbits/sec    1   3.25 MBytes       
[  5]   4.00-5.00   sec  9.62 MBytes  80.7 Mbits/sec    5   3.25 MBytes       
[  5]   5.00-6.00   sec  9.50 MBytes  79.7 Mbits/sec    0   3.25 MBytes       
[  5]   6.00-7.00   sec  9.62 MBytes  80.7 Mbits/sec    0   3.25 MBytes       
[  5]   7.00-8.00   sec  9.50 MBytes  79.7 Mbits/sec    0   3.25 MBytes       
[  5]   8.00-9.00   sec  9.62 MBytes  80.7 Mbits/sec    0   3.25 MBytes       
[  5]   9.00-10.00  sec  9.50 MBytes  79.7 Mbits/sec    0   3.25 MBytes       
```
