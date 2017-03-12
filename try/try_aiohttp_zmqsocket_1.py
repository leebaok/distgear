# -*- coding: utf-8 -*-
"""
    try_aiohttp_zmqsocket_1.py / master
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
from aiohttp import web
import zmq.asyncio
from collections import deque

# results to store events results, just for test
results = deque()

# web.Server will create a new handler--coroutine/task for every request
async def handler(request):
    print('request:'+str(request.url))
    print('send test event')
    ret = await pub.send_multipart([str.encode('test-event')])
    print('### send result ###')
    print(ret)
    future = asyncio.Future()
    results.append(future)
    print('wait result')
    await future
    print('response: '+future.result())
    return web.Response(text='event done')

# pullin works in a loop or create new coroutines for every receive ??
# now, we do not create new coroutines
async def pullin():
    while(True):
        print('wait for msg')
        msg = await pull.recv_multipart()
        msg = [ bytes.decode(x) for x in msg ]
        print('get msg: '+str(msg))
        future = results.popleft()
        print('set result')
        future.set_result('test-event done')

addr = '127.0.0.1'

# use zmq asyncio event loop to replace default loop of asyncio
loop = zmq.asyncio.ZMQEventLoop()
asyncio.set_event_loop(loop)

# call server() will create a Protocol
server = web.Server(handler)
# create_server will return a coroutine
coro = loop.create_server(server, addr, 8000)
# loop.run_until_complete will run coro to finish server creating
loop.run_until_complete(coro)
print("== Server is Created and begin to server ... ==")

# create zmq sockets
context = zmq.asyncio.Context()
pub = context.socket(zmq.PUB)
pub.bind('tcp://'+addr+':8001')
pull = context.socket(zmq.PULL)
pull.bind('tcp://'+addr+':8002')
# create task waiting for socket read event
asyncio.ensure_future(pullin())

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

tasks = asyncio.Task.all_tasks(loop)
for task in tasks:
    task.cancel()

print('-------------------------------------------------')
print(tasks)
print('-------------------------------------------------')

loop.run_until_complete(asyncio.wait(list(tasks)))

print(tasks)
loop.close()
