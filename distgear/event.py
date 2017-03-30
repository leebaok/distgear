# -*- coding: utf-8 -*-

__all__ = ['Event']

import asyncio
import json

from .log import logger


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
        Now, we only support worker to undo actions
        so, rollback only could be used when the event is to send commands to workers
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
            based on the graph and topological sorting, we can run commands correctly
        """
        graph = {}
        for key in commands:
            graph[key] = [ [], 0 ]
        for key in commands:
            deps = commands[key][3]
            graph[key][1] = len(deps)
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
        tasknames, ready, pendtasks, results = {}, [], [], {}
        for key in commands:
            if graph[key][1] == 0:
                ready.append(key)
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
        """command result and its rollback action:
            STATUS          RESULT            ROLLBACK
            -------         -------           ---------
            success         result            undo
            wait            not run           --
            *timeout         timeout           ?? (for timeout, when rollback, undo it or nothing)
            undo            undo              --
            fail            fail              --

        * means we donot support now
        """
        """for rollback: rollback the successful commands:
            commands :
                'a':('node-1', 'act-1', 'para-1', [])
                'b':('node-2', 'act-2', 'para-2', [])
                'c':('node-3', 'act-3', 'para-3', ['a', 'b'])
                'd':('node-4', 'act-4', 'para-4', ['c'])
            when 'a','b','c' run successfully and 'd' runs failed
            build back graph of 'a','b','c':
                command name       preceding      succeeding count
                 'a'               []               1 
                 'b'               []               1
                 'c'               ['a','b']        0
            based on the back graph and topological sorting, we can rollback commands in correct sequence
        """
        if not stop:
            logger.info('result:%s', str(results))
            return results
        # stop==True means rollback and some command runs failed
        # now, do rollback work
        logger.info('RollBack begin ...')
        undocmds = []
        for key in results:
            if results[key]['status']=='success':
                undocmds.append(key)
        backgraph = {}
        for key in undocmds:
            backgraph[key] = [ [], 0 ]
        for key in undocmds:
            deps = commands[key][3]
            for dep in deps:
                backgraph[key][0].append(dep)
                backgraph[dep][1] = backgraph[dep][1]+1
        tasknames, ready, pendtasks = {}, [], []
        for key in undocmds:
            if backgraph[key][1] == 0:
                ready.append(key)
        while(ready or pendtasks):
            logger.info('ready:%s', str(ready))
            logger.info('pendtasks:%s', str(pendtasks))
            for x in ready:
                node, cmd, paras, _ = commands[x]
                command = (node, 'undo@'+cmd, paras)
                logger.info('create task for:%s', str(command))
                task = asyncio.ensure_future(self.run_command(command))
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
                    results[name] = {'status':'undo', 'result':task.result()}
                    for prec in backgraph[name][0]:
                        backgraph[prec][1] = backgraph[prec][1]-1
                        if backgraph[prec][1] == 0:
                            ready.append(prec)

        logger.info('result:%s', str(results))
        return results

    async def run_command(self, command):
        """run one command, command : (node, action, parameters)
        """
        # TODO : will ZMQ ensure the message arriving the target node? 
        #        if not, should we retry some times for one command?
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
        
