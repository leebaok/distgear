# -*- coding: utf-8 -*-

__all__ = ['Master', 'SuperMaster']

import asyncio
from aiohttp import web 
import zmq.asyncio
import json

from .log import logger
from .event import Event

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

class SuperMaster(BaseMaster):
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

class Master(BaseMaster):
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

