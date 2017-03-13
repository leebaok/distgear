# -*- coding: utf-8 -*-

import sys
import random

sys.path.append("..")
from distgear.app import Master

master = Master()

# define handlers for master
@master.register('myevent')
async def testevent(event, master):
    nodes = list(master.workerinfo.keys())
    if len(nodes)==0:
        return {'result':'no workers', 'status':'fail'}
    node = nodes[random.randint(0, len(nodes)-1)]
    event.add_commands([(node, 'myaction', '100')])
    result = await event.run()
    return { 'result':result[0], 'status':'success' }

master.start()
