# -*- coding: utf-8 -*-

import sys
import random

sys.path.append("..")
import distgear

master = distgear.Master('master', debug=False)

# define handlers for master
@master.handleEvent('subevent')
async def testevent(event, master):
    print('Master: subevent')
    nodes = list(master.nodeinfo.keys())
    if len(nodes)==0:
        return {'result':'no workers', 'status':'fail'}
    node = nodes[random.randint(0, len(nodes)-1)]
    commands = {
                'a':(node, 'myaction', 'a', []), 
                'b':(node, 'myaction', 'b', []), 
                'c':(node, 'myaction', 'c', ['a','b']), 
                'd':(node, 'myaction', 'd', ['c']), 
                'e':(node, 'myaction', 'e', []), 
                'f':(node, 'myaction', 'f', ['d','e']), 
            }
    results = await event.run(commands, rollback=False, command_timeout=2, command_retry=3)
    print('Master: subevent with result:%s' % str(results))
    return { 'result':results, 'status':'success' }

master.start()
