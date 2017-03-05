import asyncio
import zmq.asyncio

async def work(msg):
    print('process event:'+str(msg))
    await asyncio.sleep(10)
    print('after sleep')
    await push.send_multipart([str.encode('event done')])

async def subin():
    while(True):
        msg = await sub.recv_multipart()
        msg = [ bytes.decode(x) for x in msg ]
        print('get msg:'+str(msg))
        asyncio.ensure_future(work(msg))

addr = '127.0.0.1'

# use zmq asyncio event loop to replace default loop of asyncio
loop = zmq.asyncio.ZMQEventLoop()
asyncio.set_event_loop(loop)

# create zmq sockets
context = zmq.asyncio.Context()
sub = context.socket(zmq.SUB)
sub.connect('tcp://'+addr+':8001')
# set SUB Topic is necessary. (otherwise, no message will be received)
sub.setsockopt(zmq.SUBSCRIBE, b"")
push = context.socket(zmq.PUSH)
push.connect('tcp://'+addr+':8002')
# create task waiting for socket read event
asyncio.ensure_future(subin())

print('event loop runs')
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass
loop.close()
