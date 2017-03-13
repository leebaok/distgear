# -*- coding: utf-8 -*-
"""
    app.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~
              Master                  Worker
         +--------------+        +-------------+
         |    Handler   |        |   Handler   |
         +--------------+        +-------------+ 
         |             PUB ---> SUB            |
    --> HTTP   Loop     |        |    Loop     |
         |            PULL <--- PUSH           |
         +--------------+        +-------------+

    Author: Bao Li
""" 

import asyncio
from aiohttp import web, ClientSession
import zmq.asyncio
import json
import sys,inspect

def output(self, log):
    if self:
        print(self.__class__.__name__+'.'+sys._getframe().f_back.f_code.co_name+' -- '+log)
    else:
        print(sys._getframe().f_back.f_code.co_name+' -- '+log)


class Master(object):

    def __init__(self):
        self.addr = '0.0.0.0'
        self.http_port = 8000
        self.pub_port = 8001
        self.pull_port = 8002
        self.event_handlers = {'_NodeJoin':self._nodejoin}
        self.workers = []
        self.workerinfo = {}
        self.count = 0
        self.pending = {}

    async def _nodejoin(self, event, master):
        paras = event.paras
        if 'name' not in paras:
            return {'status':'fail', 'result':'get work name failed'}
        name = paras['name']
        event.add_commands([(name, 'test', 'none')])
        result = await event.run()
        result = result[0]
        if result == 'test':
            self.workers.append(name)
            return {'status':'success', 'result':'work join success'}
        else:
            return {'status':'fail', 'result':'work reply failed'}

    def start(self):
        output(self, 'master start')
        self.loop = zmq.asyncio.ZMQEventLoop()
        asyncio.set_event_loop(self.loop)
        server = web.Server(self._http_handler)
        create_server = self.loop.create_server(server, self.addr, self.http_port)
        self.loop.run_until_complete(create_server)
        output(self, "create http server at http://%s:%s" % (self.addr, self.http_port))
        self.zmq_ctx = zmq.asyncio.Context()
        self.pub_sock = self.zmq_ctx.socket(zmq.PUB)
        self.pub_sock.bind('tcp://'+self.addr+':'+str(self.pub_port))
        self.pull_sock = self.zmq_ctx.socket(zmq.PULL)
        self.pull_sock.bind('tcp://'+self.addr+':'+str(self.pull_port))
        asyncio.ensure_future(self._pull_in())
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass
        self.stop()

    def stop(self):
        tasks = asyncio.Task.all_tasks(self.loop)
        for task in tasks:
            task.cancel()
        self.loop.run_until_complete(asyncio.wait(list(tasks)))
        self.loop.close()

    async def _http_handler(self, request):
        self.count = self.count + 1
        output(self, 'url : '+str(request.url))
        text = await request.text()
        output(self, 'request content : '+text)
        data = None
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            output(self, 'text is not json format')
            return web.Response(text = json.dumps({'result':'request invalid'}))
        if 'event' not in data or 'parameters' not in data:
            output(self, 'request invalid')
            return web.Response(text = json.dumps({'result':'request invalid'}))
        elif data['event'] not in self.event_handlers:
            output(self, 'event not defined')
            return web.Response(text = json.dumps({'result':'event undefined'}))
        else:
            output(self, 'call event handler')
            event = Event(data['event'], data['parameters'], self)
            result = await self.event_handlers[data['event']](event, self)
            output(self, 'result from handler: '+ str(result) )
            return web.Response(text = json.dumps(result))

    def add_pending(self, cmd_id, future):
        self.pending[cmd_id] = future

    async def _pull_in(self):
        while(True):
            output(self, 'waiting on pull socket')
            msg = await self.pull_sock.recv_multipart()
            msg = [ bytes.decode(x) for x in msg ]
            output(self, 'msg from pull socket: ' + str(msg))
            result = json.loads(msg[0])
            cmd_id = result['actionid']
            future = self.pending[cmd_id]
            del self.pending[cmd_id]
            future.set_result(result['result'])
     
    def register(self, event):
        """
            app = Master()
            @app.register('Event')
            def handler():
                pass

            app.register(...) will return decorator
            @decorator will decorate func
            this is the normal method to decorate func when decorator has args
        """
        def decorator(func):
            self.event_handlers[event] = func
            return func
        return decorator

class Event(object):
    def __init__(self, name, paras,master):
        self.master = master
        self.event_id = master.count
        self.name = name
        self.commands = []
        self.cmd_id = 0
        self.paras = paras
    def add_commands(self, commands):
        """
            commands now is list of command
        """
        self.commands = self.commands + commands 
    async def run(self):
        output(self, 'run commands: ' + str(self.commands))
        #done, pending = asycnio.wait([ self._run_command(cmd) for cmd in self.commands ])
        tasks = [ asyncio.ensure_future(self._run_command(cmd)) for cmd in self.commands ]
        done, pending = await asyncio.wait(tasks)
        self.commands = []
        return [ task.result() for task in tasks ]
    async def _run_command(self, command):
        """
            command : (node, action, parameters)
        """
        output(self, 'run command: '+str(command))
        node, action, parameters = command
        self.cmd_id = self.cmd_id + 1
        actionid = str(self.event_id) + '-' + str(self.cmd_id)
        msg = json.dumps({'action':action, 'parameters':parameters, 'actionid':actionid})
        # send (topic, msg)
        await self.master.pub_sock.send_multipart([str.encode(node), str.encode(msg)])
        future = asyncio.Future()
        self.master.add_pending(actionid, future)
        await future
        return future.result()
        

class Worker(object):
    def __init__(self, name, master_addr):
        self.master = master_addr
        self.master_http_port = 8000
        self.master_pub_port = 8001
        self.master_pull_port = 8002
        self.name = name
        self.action_handlers = {'test':self.test}
        self.pending_handlers = {}

    async def test(self, paras):
        return 'test'

    def start(self):
        self.loop = zmq.asyncio.ZMQEventLoop()
        asyncio.set_event_loop(self.loop)
        self.zmq_ctx = zmq.asyncio.Context()
        self.sub_sock = self.zmq_ctx.socket(zmq.SUB)
        self.sub_sock.connect('tcp://'+self.master+':'+str(self.master_pub_port))
        # set SUB topic is necessary (otherwise, no message will be received)
        self.sub_sock.setsockopt(zmq.SUBSCRIBE, str.encode(self.name))
        # 'all' event maybe not supported. because the result is not easy to collect
        #self.sub_sock.setsockopt(zmq.SUBSCRIBE, str.encode('all'))
        self.push_sock = self.zmq_ctx.socket(zmq.PUSH)
        self.push_sock.connect('tcp://'+self.master+':'+str(self.master_pull_port))
        asyncio.ensure_future(self._sub_in())
        asyncio.ensure_future(self._join())
        output(self, 'event loop runs')
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass
        self.stop()

    def stop(self):
        tasks = asyncio.Task.all_tasks(self.loop)
        for task in tasks:
            task.cancel()
        self.loop.run_until_complete(asyncio.wait(list(tasks)))
        self.loop.close()

    async def _join(self):
        output(self, 'worker try to join master')
        async with ClientSession() as session:
            url = 'http://'+self.master+':'+str(self.master_http_port)
            data = { 'event':'_NodeJoin', 'parameters':{'name':self.name} }
            async with session.post(url, data=json.dumps(data)) as response:
                result = await response.text()
                result = json.loads(result)
                if result['status'] == 'fail':
                    output(self, 'join master failed')
                    self.stop()
                elif result['status'] == 'success':
                    output(self, 'join master success')
                    for key in self.pending_handlers:
                        self.action_handlers[key] = self.pending_handlers[key]

    async def _sub_in(self):
        while(True):
            msg = await self.sub_sock.recv_multipart()
            msg = [ bytes.decode(x) for x in msg ]
            output(self, 'get message from sub: ' + str(msg))
            action = json.loads(msg[1])
            asyncio.ensure_future(self._run_action(action))

    async def _run_action(self, action):
        if action['action'] not in self.action_handlers:
            action['result'] = 'action not defined'
        else:
            result = await self.action_handlers[action['action']](action['parameters'])
            action['result'] = result
        await self.push_sock.send_multipart([ str.encode(json.dumps(action)) ])

    def register(self, action):
        def decorator(func):
            self.pending_handlers[action] = func
            return func
        return decorator


