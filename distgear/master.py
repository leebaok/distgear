# -*- coding: utf-8 -*-

__all__ = ['Master', 'SuperMaster']

import asyncio
from aiohttp import web 
import zmq.asyncio
import copy
import json

#from .log import logger, initLogger
# 'from .log import logger, initLogger' not work, because we call initLogger 
# in the current file. before we call initLogger, log.logger is None, so 
# 'from .log imoprt logger' will set logger as None. And then we call
# initLogger will init the log.logger but not logger in this file
from . import log
from .event import Event
from .utils import zmq_send, zmq_recv

logger = None

async def nodejoin(event, master):
    """new node joins, BaseMaster internal event. this event should be 
    raised from pull socket. we will send '@join' command to the new node 
    to test pub-sub channel and then wait for its reply
    """
    paras = event.paras
    if 'name' not in paras:
        logger.warning('one node wants to join but without name')
        return {'status':'fail', 'result':'no node name'}
    name = paras['name']
    command = (name, '@join', 'none')
    cmd_ret = await event.run_command(command)
    if cmd_ret['status'] == 'success':
        logger.info('new node:%s trys to join ...', name)
        init_ret = await master.processEvent({'event':'@NewNode', 'parameters':name})
        if init_ret['status'] == 'success':
            master.add_node(name)
            logger.info('new node:%s joins success', name)
            return {'status':'success', 'result':'work join success'}
    return {'status':'fail', 'result':'work reply failed'}

async def newnode(event, master):
    """Default do nothing. 
    You should use master.handleEvent('@NewNode') to define your new node init event
    """
    return {'status':'success', 'result':'do nothing'}

async def heartbeat(event, master):
    """heartbeat event. BaseMaster internal event.
    send '@nodeinfo' message to collect nodes information
    """
    nodes = master.get_nodes()
    commands={}
    for node in nodes:
        commands[node] = (node, '@nodeinfo', None, [])
    results = await event.run(commands)
    for node in nodes:
        ret = results[node]
        if ret.get('status', 'fail') == 'success' and 'result' in ret:
            master.set_nodeinfo(node, ret['result'])
        else:
            logger.warning('get node:%s heartbeat and info failed', node)
            master.set_nodeinfo(node, None)
    logger.info('Worker info:%s', str(master.get_nodeinfo()))
    master.raiseEvent({'event':'@HeartBeat', 'parameters':None}, delay=5)
    # heartbeat is raised by raiseEvent. its return is nouse, no one can get it
    # but the return is necessary. because raiseEvent will call processEvent 
    # and processEvent need event to return result
    return {'status':'success', 'result':'heartbeat return nothing'}


class BaseMaster(object):
    """Master and SuperMaster are subclass of BaseMaster
        +-----------+
        |   Event   |
        |  Handler  |
        +-----------+
       ??          PUB ------- commands ------>
       ??    Loop   |
       ??         PULL <--- events,results ----
        +-----------+
    """
    def __init__(self, pub_addr='0.0.0.0:8001', pull_addr='0.0.0.0:8002', debug=False):
        # init logger
        global logger
        log.initLogger(debug)
        logger = log.logger
        # init publish and pull sockets 
        self.pub_addr = pub_addr
        self.pull_addr = pull_addr
        self.loop = zmq.asyncio.ZMQEventLoop()
        asyncio.set_event_loop(self.loop)
        self.zmq_ctx = zmq.asyncio.Context()
        self.pub_sock = self.zmq_ctx.socket(zmq.PUB)
        self.pub_sock.bind('tcp://'+self.pub_addr)
        self.pull_sock = self.zmq_ctx.socket(zmq.PULL)
        self.pull_sock.bind('tcp://'+self.pull_addr)
        # init some data structure 
        self.event_handlers = {}
        self._init_handlers()
        self.nodes = []
        self.nodeinfo = {}
        self.futures = {}
        # init some tasks
        asyncio.ensure_future(self._pull_in())

    def _init_handlers(self):
        self.event_handlers['@NodeJoin'] = nodejoin
        self.event_handlers['@NewNode'] = newnode
        self.event_handlers['@HeartBeat'] = heartbeat

    def add_node(self, name):
        if name in self.nodes:
            logger.warning('%s is already in nodes list', name)
        else:
            logger.info('%s is added in nodes list', name)
            self.nodes.append(name)

    def start(self):
        logger.info('BaseMaster start ...')
        self.raiseEvent({'event':'@HeartBeat', 'parameters':None}, delay=5)
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass
        self.stop()

    def stop(self):
        logger.info('BaseMaster stop ...')
        tasks = asyncio.Task.all_tasks(self.loop)
        for task in tasks:
            task.cancel()
        self.loop.run_until_complete(asyncio.wait(list(tasks)))
        self.loop.close()

    def get_nodes(self):
        nodes = [ node for node in self.nodes ]
        return nodes

    def set_nodeinfo(self, name, info):
        if name not in self.nodes:
            logger.warning('set info of not unknown node:%s', name)
        self.nodeinfo[name] = info

    def get_nodeinfo(self):
        nodeinfo = copy.deepcopy(self.nodeinfo)
        return nodeinfo

    async def processEvent(self, eventinfo):
        """process event with event handler. eventinfo is a dict.
        eventinfo =    {'event':name, 'parameters':parameters}
                    or {'event':name, 'parameters':parameters, 'id':eventid}
        """
        if type(eventinfo) is not dict:
            return {'status':'fail', 'result':'event info is not valid'}
        name = eventinfo.get('event', None)
        paras = eventinfo.get('parameters', None)
        eventid = eventinfo.get('id', None)
        if name is None or name not in self.event_handlers:
            return {'status':'fail', 'result':'event name is not valid'}
        event = Event(name, paras, self, eventid = eventid)
        result = await self.event_handlers[name](event, self)
        if 'status' not in result or 'result' not in result:
            return {'status':'fail', 'result':result}
        else:
            return result

    def _createTask(self, eventinfo, future):
        """create a new task to process the event. when the task is done, 
        call the lambda function to put its result to the future
        """
        task = asyncio.ensure_future(self.processEvent(eventinfo))
        task.add_done_callback(lambda thistask: future.set_result(thistask.result()))

    def raiseEvent(self, eventinfo, delay=0):
        """create a new task to process event after delay seconds.
        return a future. if you want to know the result of event, 
        you can get it from the future
        """
        future = asyncio.Future()
        self.loop.call_later(delay, self._createTask, eventinfo, future)
        return future 
   
    def add_future(self, cmd_id, future):
        """add future to self.futures and pull socket will get the result and 
        set the future
        """
        self.futures[cmd_id] = future

    async def send_command(self, node, cmd, paras, cmd_id):
        msg = {'command':cmd, 'parameters':paras, 'id':cmd_id}
        # send (topic, msg)
        await zmq_send(self.pub_sock, msg, topic=node)
        future = asyncio.Future()
        self.add_future(cmd_id, future)
        await future
        return future.result()

    async def _pull_in(self):
        """pull socket receive two types of messages: command result, event request
        """
        while(True):
            logger.info('waiting on pull socket')
            content = await zmq_recv(self.pull_sock)
            if 'event' in content:
                self.raiseEvent(content)
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
    """SuperMaster
               +-----------+
               |   Event   |
               |  Handler  |
               +-----------+
               |          PUB ------- commands ------>
    events -> HTTP  Loop   |
               |         PULL <--- events,results ----
               +-----------+
    """
    def __init__(self, http_addr='0.0.0.0:8000', pub_addr='0.0.0.0:8001', pull_addr='0.0.0.0:8002', debug=False):
        """Master add a http server based on BaseMaster
        """
        BaseMaster.__init__(self, pub_addr=pub_addr, pull_addr=pull_addr, debug=debug)
        self.http_addr = http_addr
        addr, port = http_addr.split(':')
        server = web.Server(self._http_handler)
        create_server = self.loop.create_server(server, addr, int(port))
        self.loop.run_until_complete(create_server)
        logger.info('create http server at http://%s', self.http_addr)

    async def _http_handler(self, request):
        """handle http request. http request is to create event and then 
        we handle the event 
        """
        logger.debug('url:%s', str(request.url))
        text = await request.text()
        logger.info('request content:%s', text)
        data = None
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.info('text is not json format')
            return web.Response(text = json.dumps({'status':'fail', 'result':'request invalid'}))
        # web.Server will create new task of _http_handler to handle http request 
        # so, 'await self.processEvent(...)' is OK
        # we don't need to create new task to run event handler
        result = await self.processEvent(data)
        logger.info('process event with result:%s', str(result))
        return web.Response(text = json.dumps(result))


async def nodeinfo(event, master):
    return {'status':'success', 'result':master.get_nodeinfo()}

class Master(BaseMaster):
    """SuperMaster
                +-----------+
                |   Event   |
                |  Handler  |
                +-----------+
     events -> SUB         PUB ------- commands ------>
                |    Loop   |
    results <- PUSH       PULL <--- events,results ----
                +-----------+
    """
    def __init__(self, name, upper_pub_addr='0.0.0.0:8001', upper_pull_addr='0.0.0.0:8002', my_pub_addr='0.0.0.0:8003', my_pull_addr='0.0.0.0:8004', debug=False):
        BaseMaster.__init__(self, pub_addr=my_pub_addr, pull_addr=my_pull_addr, debug=debug)
        self.name = name
        self.sub_sock = self.zmq_ctx.socket(zmq.SUB)
        self.sub_sock.connect('tcp://'+upper_pub_addr)
        self.sub_sock.setsockopt(zmq.SUBSCRIBE, str.encode(self.name))
        self.push_sock = self.zmq_ctx.socket(zmq.PUSH)
        self.push_sock.connect('tcp://'+upper_pull_addr)
        # @join need to change self.status of worker, so put it inside Master
        self.event_handlers['@join'] = self._join
        self.event_handlers['@nodeinfo'] = nodeinfo
        self.status = 'waiting'
        asyncio.ensure_future(self._sub_in())
        asyncio.ensure_future(self._try_join())

    async def _join(self, event, master):
        self.status = 'working'
        return {'status':'success', 'result':'joins OK'}

    async def _try_join(self):
        msg = {'event':'@NodeJoin', 'parameters':{'name':self.name}}
        await zmq_send(self.push_sock, msg)
        await asyncio.sleep(3)
        if self.status == 'waiting':
            logger.warning('join master/supermaster failed, please check master/controller and worker...')
            self.stop()
        else:
            logger.info('join master/supermaster success')

    async def _sub_in(self):
        while(True):
            command = await zmq_recv(self.sub_sock, drop_topic=True)
            # raise Event Handler to handle the event and the handler 
            # wrap event handler to wrap the result in the valid format
            asyncio.ensure_future(self._wrapper_handler(command))

    async def _wrapper_handler(self, command):
        """wrap the event handler because we need to get the handler result and wrap it 
        in the valid format
        """
        eventinfo = { 'event': command.get('command', None),
                      'parameters': command.get('parameters', None),
                      'id': command.get('id', None)
                      }
        result = await self.processEvent(eventinfo)
        command['result'] = result.get('result', 'result not valid')
        command['status'] = result.get('status', 'fail')
        await zmq_send(self.push_sock, command)

