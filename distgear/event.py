# -*- coding: utf-8 -*-

__all__ = ['Event']

import asyncio

# for Event.driving:
#   FAIL_STOP_ALL : if someone fail, stop all tasks
#   FAIL_STOP_ONE : if someone fail, stop tasks depend on it
FAIL_STOP_ALL, FAIL_STOP_ONE = 0,1

def topoNext(graph):
    """topoNext is a generator, return ready nodes of graph by topological order.
    graph is a dict:
             Node             Succeed       Deps Count
             'a'       :       ['c']            0
             'b'       :       ['c']            0
             'c'       :       []               2
    """
    ready = []
    for item in graph:
        if graph[item][1] == 0:
            ready.append(item)
    while(True):
        done = yield ready
        ready.clear()
        for item in done:
            for succ in graph[item][0]:
                graph[succ][1] = graph[succ][1]-1
                if graph[succ][1] == 0:
                    ready.append(succ)
            # graph[item] is done, it will not be used
            del graph[item]
        # graph is empty, stop the generator
        # if there is a cycle in graph, the generator will run forever
        if len(graph) == 0:
            break

class Event(object):
    count = 0
    def __init__(self, name, paras, master, eventid = None):
        Event.count = Event.count+1
        self.master = master
        self.log = master.log
        if not eventid:
            self.id = Event.count
        else:
            self.id = eventid
        self.name = name
        self.cmd_cnt = 0
        self.paras = paras

    async def run(self, commands, rollback=False, command_timeout=30, command_retry=3):
        """run multi commands, commands is a dict
        Now, we only support worker to undo actions
        so, rollback only could be used when the event is to send commands to workers
        """
        self.log.debug('run commands:%s', str(commands))
        self.log.info('run commands:%s', str(commands))
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
        self.log.debug('graph:%s', str(graph))
        self.log.info('graph:%s', str(graph))
        if rollback:
            stop = FAIL_STOP_ALL
        else:
            stop = FAIL_STOP_ONE
        (hasfail, results) = await self.driving(commands, graph, undo=False, command_timeout=command_timeout, command_retry=command_retry, stop=stop)
        self.log.info('run result:%s', str(results))
        if not hasfail or not rollback:
            return results
        self.log.debug('RollBack begin ...')
        self.log.info('RollBack begin ...')
        """command result and its rollback action:
            STATUS          RESULT            ROLLBACK
            -------         -------           ---------
            success         result            undo
            wait            not run           --
            timeout         timeout           -- 
            undo            undo              --
            fail            fail              --
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
        self.log.info('back graph:%s', str(backgraph))
        (hasfail, undoresults) = await self.driving(commands, backgraph, undo=True, command_timeout=command_timeout, command_retry=command_retry, stop=FAIL_STOP_ONE)
        self.log.info('undo result:%s', str(results))
        for key in undoresults:
            results[key] = {'status':'undo', 'result':undoresults[key]}
        self.log.info('final result:%s', str(results))
        return results
    
    async def driving(self, commands, graph, undo=False, command_timeout=30, command_retry=3, stop=FAIL_STOP_ONE):
        if len(graph) == 0:
            return (False, {})
        needrun = list(graph.keys())
        toporun = topoNext(graph)
        ready = toporun.send(None)
        pendtasks = []
        tasknames, results = {}, {}
        hasfail = False
        while(ready or pendtasks):
            self.log.debug('ready tasks:%s', str(ready))
            self.log.debug('pend tasks:%s', str(pendtasks))
            for x in ready:
                node, cmd, paras = commands[x][:3]
                if undo:
                    cmd = 'undo@'+cmd
                self.log.debug('create task for (%s, %s, %s)', node, cmd, paras)
                task = asyncio.ensure_future(self.run_command((node, cmd, paras), timeout=command_timeout, retry=command_retry))
                tasknames[task] = x
                pendtasks.append(task)
            ready.clear()
            if not pendtasks:
                continue
            self.log.debug('wait for:%s', str(pendtasks))
            done, pend = await asyncio.wait(pendtasks, return_when=asyncio.FIRST_COMPLETED)
            self.log.debug('task done:%s', str(done))
            success = []
            for task in done:
                pendtasks.remove(task)
                name = tasknames[task]
                result = task.result()
                if 'status' not in result:
                    result = {'status':'fail', 'result':result}
                results[name] = result
                if result['status'] != 'success':
                    hasfail = True
                else:
                    success.append(name)
            if not hasfail or (hasfail and stop == FAIL_STOP_ONE):
                try:
                    ready = toporun.send(success)
                except StopIteration:
                    ready = []
        for key in needrun:
            if key not in results:
                results[key] = {'status':'wait', 'result':'dependent commands not run success'}
        return (hasfail, results)

    async def run_command(self, command, timeout=30, retry=1, retry_if_fail=False):
        """run one command, command : (node, command, parameters)
            :retry -- times to retry command, retry when timeout by default
            :retry_if_fail -- if runs failed, retry command
        """
        if len(command) != 3:
            return {'status':'fail', 'result':'command not valid'}
        node, cmd, paras = command
        if retry <= 0:
            retry = 1
        for i in range(retry):
            self.cmd_cnt = self.cmd_cnt + 1
            cmd_id = str(self.id) + '-' + str(self.cmd_cnt)
            self.log.debug('run command: %s with id: %s', str(command), str(cmd_id))
            result = await self.master.send_command(node, cmd, paras, cmd_id, timeout=timeout)
            if result['status'] == 'timeout' or (retry_if_fail and result['status'] == 'fail'):
                pass
            else:
                break
        return result
        
