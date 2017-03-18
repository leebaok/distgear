# some ideas when coding

## await send/recv vs. add_reader/writer
when we use asyncio eventloop to watch zmq sockets, we can do it in two ways:
1. await zmq.asyncio.context.socket.send/recv
	it is easy to use. but every time we await, the Future maybe put in/out 
	callbacks again and again. Does it has side effect on the performance ?
2. add_reader/writer with callbacks
	for reader, this is easy to use. but for writer, when write is available, 
	we maybe have not data to write. so we need to do some more actions for 
	writing.
now we use the first method for zmq sockets. because it is easy to use.

## should we support 'ALL' topic for pub-sub
now we donot support 'ALL' topic for pub-sub. Because we donot know how to
collect the results of 'ALL' type event
