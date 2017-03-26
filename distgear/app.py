# -*- coding: utf-8 -*-
"""
    app.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~

             Master                Controller               Worker
         +-----------+           +-----------+           +-----------+
         |   Event   |           |   Event   |           |  Action   |
         |  Handler  |           |  Handler  |           |  Handler  |
         +-----------+           +-----------+           +-----------+
         |          PUB ----+-> SUB         PUB ----+-> SUB          |
   --> HTTP   Loop   |      |    |    Loop   |      |    |    Loop   |
         |         PULL <-+-|-- PUSH       PULL <-+-|-- PUSH         |
         +-----------+    | |    +-----------+    | |    +-----------+
                          | |                     | |
                          | |        Worker       | |       Worker
                          | |    +-----------+    | |    +-----------+
                          | |    |  Action   |    | |    |  Action   |
                          | |    |  Handler  |    | |    |  Handler  |
                          | |    +-----------+    | |    +-----------+
                          | +-> SUB          |    | +-> SUB          |
                          |      |    Loop   |    |      |    Loop   |
                          +---- PUSH         |    +---- PUSH         |
                                 +-----------+           +-----------+

    Author: Bao Li
""" 

import asyncio
from aiohttp import web, ClientSession
import zmq.asyncio
import json
import sys,inspect
import psutil
from log import logger

class BaseMaster(object):
    """Master and Controller are subclass of BaseMaster
      +-----------+
      |   Event   |
      |  Handler  |
      +-----------+
     ??          PUB ------- commands ------>
     ??    Loop   |
     ??         PULL <--- events,results ----
      +-----------+
    """
    def __init__(self, pub_addr='0.0.0.0:8001', pull_addr='0.0.0.0:8002'):
        logger.info('BaseMaster init ...')
        # init base configuration
        self.pub_addr = pub_addr
        self.pull_addr = pull_addr
        self.event_handlers = {'@NodeJoin':self._nodejoin}
        self.nodes = []
        self.nodeinfo = {}
        self.futures = {}
        # init loop, sockets and tasks
        self.loop = zmq.asyncio.ZMQEventLoop()
        asyncio.set_event_loop(self.loop)
        self.zmq_ctx = zmq.asyncio.Context()
        self.pub_sock = self.zmq_ctx.socket(zmq.PUB)
        self.pub_sock.bind('tcp://'+self.pub_addr)
        self.pull_sock = self.zmq_ctx.socket(zmq.PULL)
        self.pull_sock.bind('tcp://'+self.pull_addr)
        asyncio.ensure_future(self._pull_in())
        asyncio.ensure_future(self._heartbeat())

    async def _nodejoin(self, event, master):
        """new node joins, this event should be raised from pull socket.
        here, we will send '@join' command to the new node to test pub-sub channel
        and then wait for its reply
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
            self.nodes.append(name)
            logger.info('new node joins:%s', name)
            # return is not necessary, because this event is raised by pull socket
            # and it don't need a result
            return {'status':'success', 'result':'work join success'}
        else:
            return {'status':'fail', 'result':'work reply failed'}

    def start(self):
        logger.info('basemaster start ...')
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass
        self.stop()

    def stop(self):
        logger.info('basemaster stop ...')
        tasks = asyncio.Task.all_tasks(self.loop)
        for task in tasks:
            task.cancel()
        self.loop.run_until_complete(asyncio.wait(list(tasks)))
        self.loop.close()
   
    async def _heartbeat(self):
        """heartbeat event, send 'nodeinfo' message to collect nodes information
        """
        while(True):
            await asyncio.sleep(20)
            nodes = [ node for node in self.nodes ]
            event = Event('@HeartBeat', 'Nothing', self)
            commands={}
            for node in nodes:
                commands[node] = (node, '@nodeinfo', 'Nothing', [])
            results = await event.run(commands)
            for node in nodes:
                if results[node]['status'] == 'success':
                    self.nodeinfo[node] = results[node]['result']
                else:
                    logger.warning('get node:%s heartbeat and info failed', node)
                    self.nodeinfo[node] = None
            logger.info('Worker info:%s', str(self.nodeinfo))

    def add_future(self, cmd_id, future):
        """add future to self.futures and pull socket will get the result and 
        set the future
        """
        self.futures[cmd_id] = future

    async def _pull_in(self):
        """pull socket receive two types of messages: command result, event request
        """
        while(True):
            logger.info('waiting on pull socket')
            msg = await self.pull_sock.recv_multipart()
            msg = [ bytes.decode(x) for x in msg ]
            logger.info('message from pull socket:%s', str(msg))
            content = json.loads(msg[0])
            if 'event' in content:
                event = Event(content['event'], content['parameters'], self)
                asyncio.ensure_future(self.event_handlers[content['event']](event, self))
            else:
                result = content
                cmd_id = result['id']
                future = self.futures[cmd_id]
                del self.futures[cmd_id]
                future.set_result({'status':result['status'], 'result':result['result']})
     
    def handleEvent(self, event):
        """register handler of event
        Usage: 
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

class Master(BaseMaster):
    def __init__(self, http_addr='0.0.0.0:8000', pub_addr='0.0.0.0:8001', pull_addr='0.0.0.0:8002'):
        """Master add a http server based on BaseMaster
        """
        BaseMaster.__init__(self, pub_addr=pub_addr, pull_addr=pull_addr)
        self.http_addr = http_addr
        addr,port = http_addr.split(':')
        server = web.Server(self._http_handler)
        create_server = self.loop.create_server(server, addr, int(port))
        self.loop.run_until_complete(create_server)
        logger.info('create http server at http://%s', self.http_addr)

    async def _http_handler(self, request):
        """handle http request. http request is to raise event and then 
        we handle the event 
        """
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
            logger.info('process event:%s with id:%s', data['event'], str(event.id))
            # web.Server will create new task of _http_handler to handle http request 
            # so, 'await self.event_handlers[...](...)' is OK
            # we don't need to create new task to run event handler
            result = await self.event_handlers[data['event']](event, self)
            logger.info('result from handler:%s', str(result))
            return web.Response(text = json.dumps(result))

class Controller(BaseMaster):
    def __init__(self, name, upper_pub_addr='0.0.0.0:8001', upper_pull_addr='0.0.0.0:8002', my_pub_addr='0.0.0.0:8003', my_pull_addr='0.0.0.0:8004'):
        BaseMaster.__init__(self, pub_addr=my_pub_addr, pull_addr=my_pull_addr)
        self.name = name
        self.sub_sock = self.zmq_ctx.socket(zmq.SUB)
        self.sub_sock.connect('tcp://'+upper_pub_addr)
        self.sub_sock.setsockopt(zmq.SUBSCRIBE, str.encode(self.name))
        self.push_sock = self.zmq_ctx.socket(zmq.PUSH)
        self.push_sock.connect('tcp://'+upper_pull_addr)
        self.event_handlers['@join'] = self._join
        self.event_handlers['@nodeinfo'] = self._nodeinfo
        self.status = 'waiting'
        asyncio.ensure_future(self._sub_in())
        asyncio.ensure_future(self._try_join())

    async def _join(self, event, master):
        self.status = 'working'
        return {'status':'success', 'result':'joins OK'}

    async def _nodeinfo(self, event, master):
        return {'status':'success', 'result':self.nodeinfo}

    async def _try_join(self):
        msg = {'event':'@NodeJoin', 'parameters':{'name':self.name}}
        await self.push_sock.send_multipart([str.encode(json.dumps(msg))])
        await asyncio.sleep(3)
        if self.status is 'waiting':
            logger.warning('join master/controller failed, please check master/controller and worker...')
            self.stop()
        else:
            logger.info('join master/controller success')

    async def _sub_in(self):
        while(True):
            msg = await self.sub_sock.recv_multipart()
            msg = [ bytes.decode(x) for x in msg ]
            logger.info('get message from sub:%s', str(msg))
            command = json.loads(msg[1])
            # raise Event Handler to handle the event and the handler 
            # wrap event handler to wrap the result in the valid format
            asyncio.ensure_future(self._wrapper_handler(command))

    async def _wrapper_handler(self, command):
        """wrap the event handler because we need to get the handler result and wrap it 
        in the valid format
        """
        if ('command' or 'parameters' or 'id') not in command:
            command['result'] = 'invalid command'
            command['status'] = 'fail'
        elif command['command'] not in self.event_handlers:
            command['result'] = 'event not defined'
            command['status'] = 'fail'
        else:
            event = Event(command['command'], command['parameters'], self, eventid=command['id'])
            result = await self.event_handlers[command['command']](event, self)
            command['result'] = result['result']
            command['status'] = result['status']
        await self.push_sock.send_multipart([ str.encode(json.dumps(command)) ])


class Event(object):
    count = 0
    def __init__(self, name, paras, master, eventid = None):
        Event.count = Event.count+1
        self.master = master
        if not eventid:
            self.id = Event.count
        else:
            self.id = eventid
        self.name = name
        self.cmd_cnt = 0
        self.paras = paras

    async def run_without_rollback(self, commands):
        """run multi commands, commands is a dict
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

    async def run(self, commands, rollback=False):
        """run multi commands, commands is a dict
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
            if some task runs failed:
                if rollback is False, the tasks depending on it will not run
                if rollback is True, all done tasks will be undid
            by the way, if some task failed, it means the event/action on the
            remote node is failed. And the remote event/action should clear the 
            things it has done
        """
        stop = False
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
                    result = task.result()
                    if stop or ('status' not in result) or (result['status'] == 'fail'):
                        if rollback:
                            stop = True
                        continue
                    for succ in graph[name][0]:
                        graph[succ][1] = graph[succ][1]-1
                        if graph[succ][1] == 0:
                            ready.append(succ)
        for key in graph:
            if graph[key][1]!=0:
                results[key] = {'status':'wait', 'result':'dependent commands run failed or not run or some command failed with rollback mode'}
        # stop==True means rollback and some command runs failed
        if stop:
            pass

        logger.info('result:%s', str(results))
        return results


    async def run_command(self, command):
        """run one command, command : (node, action, parameters)
        """
        node, action, parameters = command
        self.cmd_cnt = self.cmd_cnt + 1
        cmd_id = str(self.id) + '-' + str(self.cmd_cnt)
        logger.info('run command: %s with id: %s', str(command), str(cmd_id))
        msg = json.dumps({'command':action, 'parameters':parameters, 'id':cmd_id})
        # send (topic, msg)
        await self.master.pub_sock.send_multipart([str.encode(node), str.encode(msg)])
        future = asyncio.Future()
        self.master.add_future(cmd_id, future)
        await future
        return future.result()
        

class Worker(object):
    def __init__(self, name, master_pub_addr='127.0.0.1:8003', master_pull_addr='127.0.0.1:8004'):
        logger.info('init worker ...')
        # init base configurations
        self.name = name
        self.master_pub_addr = master_pub_addr
        self.master_pull_addr = master_pull_addr
        self.action_handlers = {'@join':self._join}
        # init loop, sockets and tasks
        self.loop = zmq.asyncio.ZMQEventLoop()
        asyncio.set_event_loop(self.loop)
        self.zmq_ctx = zmq.asyncio.Context()
        self.sub_sock = self.zmq_ctx.socket(zmq.SUB)
        self.sub_sock.connect('tcp://'+self.master_pub_addr)
        # set SUB topic is necessary (otherwise, no message will be received)
        self.sub_sock.setsockopt(zmq.SUBSCRIBE, str.encode(self.name))
        # 'all' event maybe not supported. because the result is not easy to collect
        #self.sub_sock.setsockopt(zmq.SUBSCRIBE, str.encode('all'))
        self.push_sock = self.zmq_ctx.socket(zmq.PUSH)
        self.push_sock.connect('tcp://'+self.master_pull_addr)
        asyncio.ensure_future(self._sub_in())
        asyncio.ensure_future(self._try_join())
        self.pending_handlers = {'@nodeinfo':self._nodeinfo}

    def start(self):
        logger.info('worker start ...')
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

    async def _try_join(self):
        """try to join master/controller.
        1. push to master/controller pull socket to raise @NodeJoin event
        2. master/controller send '@join' command to this node to test pub-sub channel and push-pull channel
        3. this node do @join action for some preparation
        4. _try_join wait for some time and then test if this node joins 
        """
        msg = {'event':'@NodeJoin', 'parameters':{'name':self.name}}
        await self.push_sock.send_multipart([str.encode(json.dumps(msg))])
        await asyncio.sleep(3)
        if '@nodeinfo' not in self.action_handlers:
            logger.warning('join master/controller failed, please check master/controller and worker...')
            self.stop()
        else:
            logger.info('join master/controller success')

    async def _join(self, paras):
        for key in self.pending_handlers:
            self.action_handlers[key] = self.pending_handlers[key]
        self.pending_handlers.clear()            
        return {'status':'success', 'result':'joins OK'}

    async def _nodeinfo(self, paras):
        """nodeinfo action, return system info
        """
        memload = psutil.virtual_memory().percent
        cpuload = psutil.cpu_percent()
        return { 'status':'success', 'result': {'mem':memload, 'cpu':cpuload} }

    async def _sub_in(self):
        """receive commands from sub socket
        """
        while(True):
            # recv (topic, msg)
            msg = await self.sub_sock.recv_multipart()
            msg = [ bytes.decode(x) for x in msg ]
            logger.info('get message from sub:%s', str(msg))
            command = json.loads(msg[1])
            asyncio.ensure_future(self._run_action(command))

    async def _run_action(self, command):
        if ('command' or 'parameters' or 'id') not in command:
            command['result'] = 'invalid command'
            command['status'] = 'fail'
        elif command['command'] not in self.action_handlers:
            command['result'] = 'action not defined'
            command['status'] = 'fail'
        else:
            result = await self.action_handlers[command['command']](command['parameters'])
            command['result'] = result['result']
            command['status'] = result['status']
        await self.push_sock.send_multipart([ str.encode(json.dumps(command)) ])

    def doAction(self, action):
        """register handler of action. this is a wrapper of decorator
        """
        def decorator(func):
            self.pending_handlers[action] = func
            return func
        return decorator

