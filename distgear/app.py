# -*- coding: utf-8 -*-
"""
    app.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~
              Master                  Worker
         +--------------+        +-------------+
         |    Handler   |        |   Handler   |    ==> User Provide
         +--------------+        +-------------+ 
         |             PUB ---> SUB            |
    --> HTTP   Loop     |        |    Loop     |    ==> DistGear Provide
         |            PULL <--- PUSH           |
         +--------------+        +-------------+

    Author: Bao Li
""" 

import asyncio
from aiohttp import web, ClientSession
import zmq.asyncio
import json
import sys,inspect
import psutil
from log import logger


class Master(object):

    def __init__(self):
        self.addr = '0.0.0.0'
        self.http_port = 8000
        self.pub_port = 8001
        self.pull_port = 8002
        self.event_handlers = {'@NodeJoin':self._nodejoin}
        self.workers = []
        self.workerinfo = {}
        self.pending = {}

    async def _nodejoin_deprecated(self, event, master):
        """new node joins master and master will try to run commands to the node
        to test the node's sub and push sockets"""
        paras = event.paras
        if 'name' not in paras:
            return {'status':'fail', 'result':'get worker name failed'}
        name = paras['name']
        command = (name, '@test', 'none')
        result = await event.run_command(command)
        if result['status'] == 'success':
            self.workers.append(name)
            logger.info('new node join: %s', name)
            return {'status':'success', 'result':'work join success'}
        else:
            return {'status':'fail', 'result':'work reply failed'}

    async def _nodejoin(self, event, master):
        """new node joins, this event should be raised from pull socket.
        here, we will send '@join' command to the new node and wait for its reply
        """
        paras = event.paras
        if 'name' not in paras:
            logger.warning('one node wants to join but without name')
            # return is not necessary, because this event is raised by pull socket
            # and it don't need a result
            return {'status':'fail', 'result':'no node name'}
        name = paras['name']
        command = (name, '@join', 'none')
        result = await event.run_command(command)
        if result['status'] == 'success':
            self.workers.append(name)
            logger.info('new node joins:%s', name)
            # return is not necessary, because this event is raised by pull socket
            # and it don't need a result
            return {'status':'success', 'result':'work join success'}
        else:
            return {'status':'fail', 'result':'work reply failed'}

    def start(self):
        logger.info('master start ...')
        self.loop = zmq.asyncio.ZMQEventLoop()
        asyncio.set_event_loop(self.loop)
        server = web.Server(self._http_handler)
        create_server = self.loop.create_server(server, self.addr, self.http_port)
        self.loop.run_until_complete(create_server)
        logger.info("create http server at http://%s:%s", self.addr, self.http_port)
        self.zmq_ctx = zmq.asyncio.Context()
        self.pub_sock = self.zmq_ctx.socket(zmq.PUB)
        self.pub_sock.bind('tcp://'+self.addr+':'+str(self.pub_port))
        self.pull_sock = self.zmq_ctx.socket(zmq.PULL)
        self.pull_sock.bind('tcp://'+self.addr+':'+str(self.pull_port))
        asyncio.ensure_future(self._pull_in())
        asyncio.ensure_future(self._heartbeat())
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
        logger.info('url:%s', str(request.url))
        text = await request.text()
        logger.info('request content:%s', text)
        data = None
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.info('text is not json format')
            return web.Response(text = json.dumps({'status':'fail', 'result':'request invalid'}))
        if 'event' not in data or 'parameters' not in data:
            logger.info('request invalid')
            return web.Response(text = json.dumps({'status':'fail', 'result':'request invalid'}))
        elif data['event'] not in self.event_handlers:
            logger.info('event not defined')
            return web.Response(text = json.dumps({'status':'fail', 'result':'event undefined'}))
        else:
            logger.info('call event handler')
            event = Event(data['event'], data['parameters'], self)
            logger.info('process event:%s with id:%s', data['event'], str(event.event_id))
            # web.Server will create new task of _http_handler to handle http request 
            # so, 'await self.event_handlers[...](...)' is OK
            # we don't need to create new task to run event handler
            result = await self.event_handlers[data['event']](event, self)
            logger.info('result from handler:%s', str(result))
            return web.Response(text = json.dumps(result))
    
    async def _heartbeat(self):
        while(True):
            await asyncio.sleep(5)
            nodes = [ node for node in self.workers ]
            event = Event('_HeartBeat', 'Nothing', self)
            commands={}
            for node in nodes:
                commands[node] = (node, '@heartbeat', 'Nothing', [])
            results = await event.run(commands)
            for node in nodes:
                if results[node]['status'] == 'success':
                    self.workerinfo[node] = results[node]['result']
                else:
                    logger.warning('get node:%s heartbeat and info failed', node)
                    self.workerinfo[node] = None
            logger.info('Worker info:%s', str(self.workerinfo))

    def add_pending(self, cmd_id, future):
        self.pending[cmd_id] = future

    async def _pull_in(self):
        while(True):
            logger.info('waiting on pull socket')
            msg = await self.pull_sock.recv_multipart()
            msg = [ bytes.decode(x) for x in msg ]
            logger.info('msg from pull socket:%s', str(msg))
            content = json.loads(msg[0])
            if 'event' in content:
                event = Event(content['event'], content['parameters'], self)
                asyncio.ensure_future(self.event_handlers[content['event']](event, self))
            else:
                result = content
                cmd_id = result['actionid']
                future = self.pending[cmd_id]
                del self.pending[cmd_id]
                future.set_result({'status':result['status'], 'result':result['result']})
     
    def handleEvent(self, event):
        """
            app = Master()
            @app.handleEvent('Event')
            def handler():
                pass

            app.handleEvent(...) will return decorator
            @decorator will decorate func
            this is the normal method to decorate func when decorator has args
        """
        def decorator(func):
            self.event_handlers[event] = func
            return func
        return decorator

class Event(object):
    def __init__(self, name, paras, master):
        self.master = master
        self.event_id = master.loop.time()
        self.name = name
        self.cmd_id = 0
        self.paras = paras

    async def run(self, commands):
        """
            commands now is a dict
        """
        logger.info('run commands:%s', str(commands))
        """
            commands :
                'a':('node-1', 'act-1', 'para-1', [])
                'b':('node-2', 'act-2', 'para-2', [])
                'c':('node-3', 'act-3', 'para-3', ['a', 'b'])
            build graph from commands:
                command name       succeed       deps count
                 'a'               ['c']            0
                 'b'               ['c']            0
                 'c'               []               2
        """
        graph = {}
        ready = []
        tasknames, pendtasks, results = {}, [], {}
        for key in commands:
            graph[key] = [ [], 0 ]
        for key in commands:
            deps = commands[key][3]
            graph[key][1] = len(deps)
            if graph[key][1] == 0:
                ready.append(key)
            for dep in deps:
                graph[dep][0].append(key)
        logger.info('graph:%s', str(graph))
        """
            ready is tasks ready to run
            pendtasks is tasks running
            so, 
                step 1: run the ready tasks
                step 2: wait for some task finish and update ready queue
        """
        while(ready or pendtasks):
            logger.info('ready:%s', str(ready))
            logger.info('pendtasks:%s', str(pendtasks))
            for x in ready:
                logger.info('create task for:%s', str(commands[x]))
                task = asyncio.ensure_future(self.run_command(commands[x][:3]))
                tasknames[task] = x
                pendtasks.append(task)
            ready.clear()
            if pendtasks:
                logger.info('wait for:%s', str(pendtasks))
                done, pend = await asyncio.wait(pendtasks, return_when=asyncio.FIRST_COMPLETED)
                logger.info('task done:%s', str(done))
                for task in done:
                    pendtasks.remove(task)
                    name = tasknames[task]
                    results[name] = task.result()
                    for succ in graph[name][0]:
                        graph[succ][1] = graph[succ][1]-1
                        if graph[succ][1] == 0:
                            ready.append(succ)
        logger.info('result:%s', str(results))
        return results

    async def run_command(self, command):
        """
            command : (node, action, parameters)
        """
        node, action, parameters = command
        self.cmd_id = self.cmd_id + 1
        actionid = str(self.event_id) + '-' + str(self.cmd_id)
        logger.info('run command: %s with id: %s', str(command), str(actionid))
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
        self.action_handlers = {'@join':self._join}
        self.pending_handlers = {'@heartbeat':self._heartbeat}

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
        asyncio.ensure_future(self._try_join())
        logger.info('event loop runs')
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

    async def _join_deprecated(self):
        """Join master"""
        logger.info('worker try to join master')
        async with ClientSession() as session:
            url = 'http://'+self.master+':'+str(self.master_http_port)
            data = { 'event':'@NodeJoin', 'parameters':{'name':self.name} }
            async with session.post(url, data=json.dumps(data)) as response:
                result = await response.text()
                result = json.loads(result)
                if result['status'] == 'fail':
                    logger.info('join master failed')
                    self.stop()
                elif result['status'] == 'success':
                    logger.info('join master success')
                    for key in self.pending_handlers:
                        self.action_handlers[key] = self.pending_handlers[key]

    async def _try_join(self):
        msg = {'event':'@NodeJoin', 'parameters':{'name':self.name}}
        await self.push_sock.send_multipart([str.encode(json.dumps(msg))])
        await asyncio.sleep(3)
        if '@heartbeat' not in self.action_handlers:
            logger.warning('join master/controller failed, please check master/controller and worker...')
            self.stop()
        else:
            logger.info('join master/controller success')

    async def _join(self, paras):
        for key in self.pending_handlers:
            self.action_handlers[key] = self.pending_handlers[key]
        self.pending_handlers.clear()            
        return {'status':'success', 'result':'joins OK'}

    async def _heartbeat(self, paras):
        memload = psutil.virtual_memory().percent
        cpuload = psutil.cpu_percent()
        return { 'status':'success', 'result': {'mem':memload, 'cpu':cpuload} }


    async def _sub_in(self):
        while(True):
            # recv (topic, msg)
            msg = await self.sub_sock.recv_multipart()
            msg = [ bytes.decode(x) for x in msg ]
            logger.info('get message from sub:%s', str(msg))
            action = json.loads(msg[1])
            asyncio.ensure_future(self._run_action(action))

    async def _run_action(self, action):
        if action['action'] not in self.action_handlers:
            action['result'] = 'action not defined'
            action['status'] = 'fail'
        else:
            result = await self.action_handlers[action['action']](action['parameters'])
            action['result'] = result['result']
            action['status'] = result['status']
        await self.push_sock.send_multipart([ str.encode(json.dumps(action)) ])

    def doAction(self, action):
        def decorator(func):
            self.pending_handlers[action] = func
            return func
        return decorator


