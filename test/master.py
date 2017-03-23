# -*- coding: utf-8 -*-

import sys
import random

sys.path.append("../distgear")
from app import Master

master = Master()

# define handlers for master
@master.handleEvent('myevent')
async def testevent(event, master):
    nodes = list(master.workerinfo.keys())
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
    event.add_commands(commands)
    results = await event.run()
    return { 'result':results, 'status':'success' }

master.start()
