# -*- coding: utf-8 -*-

import sys
import random

sys.path.append("../distgear")
from app import Master

master = Master()

# define handlers for master
@master.handleEvent('myevent')
async def testevent(event, master):
    nodes = list(master.nodeinfo.keys())
    if len(nodes)==0:
        return {'result':'no workers', 'status':'fail'}
    node = nodes[random.randint(0, len(nodes)-1)]
    result = await event.run_command((node, 'subevent', 'Nothing'))
    return { 'result':result, 'status':'success' }

master.start()
