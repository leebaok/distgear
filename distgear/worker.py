# -*- coding: utf-8 -*-

__all__ = ['Worker']

import asyncio
import zmq.asyncio
import json
import psutil

from .log import logger

class Worker(object):
    def __init__(self, name, master_pub_addr='127.0.0.1:8003', master_pull_addr='127.0.0.1:8004'):
        logger.info('init worker ...')
        # init base configurations
        self.name = name
        self.master_pub_addr = master_pub_addr
        self.master_pull_addr = master_pull_addr
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
        self.action_handlers = {'@join':self._join}
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

    def undoAction(self, action):
        """register handler of undo action. this is a wrapper of decorator
        """
        def decorator(func):
            self.pending_handlers['undo@'+action] = func
            return func
        return decorator

