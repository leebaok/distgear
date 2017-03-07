# -*- coding: utf-8 -*-
"""
    try_aiohttp_zmqsocket_2.py / worker
    ~~~~~~~~~~~~~~~~~~~~~~~~~
            Master              Worker
         +----------+        +---------+
         |         PUB ---> SUB        |
    --> Http        |        |         |
         |        PULL <--- PUSH       |
         +----------+        +---------+

    Author: Bao Li
""" 

import asyncio
import zmq.asyncio
from concurrent.futures import CancelledError

async def work(msg):
    print('process event:'+str(msg))
    await asyncio.sleep(10)
    print('after sleep')
    await push.send_multipart([str.encode('event done')])

# create new coroutine for every msg/event from sub
async def subin():
    while(True):
        msg = await sub.recv_multipart()
        msg = [ bytes.decode(x) for x in msg ]
        print('get msg:'+str(msg))
        asyncio.ensure_future(work(msg))

async def subin0():
    try:
        while(True):
            msg = await sub.recv_multipart()
            msg = [ bytes.decode(x) for x in msg ]
            print('get msg:'+str(msg))
            asyncio.ensure_future(work(msg))
    except CancelledError:
        print('task is cancelled')
        pass

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

tasks = asyncio.Task.all_tasks(loop)
for task in tasks:
    task.cancel()

print('------------------------------------------')
print(tasks)
print('------------------------------------------')

# asyncio.wait will catch CancelledError from tasks and handle it
# if we use loop.run_until_complete(task), we need to catch CancelledError
#      by ourselves (catch it in task or catch it with loop.run_until_complete)
loop.run_until_complete(asyncio.wait(list(tasks)))

loop.close()

