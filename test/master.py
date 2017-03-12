# -*- coding: utf-8 -*-

import sys

sys.path.append("..")
from distgear.app import Master

master = Master()

# define handlers for master
@master.register('test')
async def testevent(event, master):
    print('handler test event')
    event.add_commands([('w1', 'test', '123')])
    result = await event.run()
    return { 'result':result[0] }

master.start()
