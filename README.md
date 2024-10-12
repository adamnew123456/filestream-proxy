# What is this?

This is a file-based TCP proxy. Create a directory for the proxy to hold its
buffers, then start a listener and a forwarder. The listener accepts a TCP
connection and the forwarder passes its data to some other host:

```bash
$ mkdir /tmp/fs-proxy
$ dotnet run listen /tmp/fs-proxy 8000 &
$ dotnet run forward /tmp/fs-proxy 1.1.1.1:80 &
$ curl -H 'Host: 1.1.1.1' http://localhost:8000
```

(HTTP isn't the best example here because of vhosting, use your imagination.
You can run SSH or anything else over this thing if you want.)

# What does it do?

It creates four files in the given directory:

- A pair of 'pipe' files. These are a collection of 'pages' (currently 64
  buffers each 8KB long), each of which has some data in it and a serial
  number for tracking ordering.

- A pair of lock files. These mediate access to the pipe files, only one
  process can access a pipe file at the same time.

The listener and forwarder each run a pair of threads, one of which copies data
out of *one* pipe file and onto a socket, and the other receives data from the
socket and writes it to the *other* pipe file. The listener and forwarder
connect their readers and writers to opposite pipes to get bidirectional
communication!

# Eck! Why?

To have a convenient reverse tunnel that I can run over RDP. If I share a
folder with the server, I can run this program on the server and on the client
and get a functional TCP connection from the server to a port on my machine.

# Is it fast?

If you need this you probably don't care, but if I put the directory on a tmpfs
and run iperf over the tunnel, I get about 8 MB/s (~60 Mbits/s). That's a best
case scenario though and in real use you'll get a lot slower.

This can probably be optimized by using a smarter locking mechanism and more
efficient file IO (you could even get really smart and do some kind of per-page
locking to reduce contention between the reader and writer), but frankly I'm glad
to have it working at all.
