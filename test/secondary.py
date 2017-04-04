# -*- coding: utf-8 -*-

import sys
import random

sys.path.append("..")
import distgear

secondary = distgear.SecondaryMaster('master', debug=False)

# define handlers for master
@secondary.handleEvent('subevent')
async def testevent(event, master):
    print('Master: subevent')
    nodes = list(master.nodeinfo.keys())
    if len(nodes)==0:
        return {'result':'no workers', 'status':'fail'}
    node = nodes[random.randint(0, len(nodes)-1)]
    commands = {
            'a':(node, 'myaction', {'time':1,'name':'a','ret':'success'}, []), 
            'b':(node, 'myaction', {'time':3,'name':'b','ret':'success'}, []), 
            'c':(node, 'myaction', {'time':1,'name':'c','ret':'success'}, ['a','b']), 
            'd':(node, 'myaction', {'time':2,'name':'d','ret':'success'}, ['c']), 
            'e':(node, 'myaction', {'time':4,'name':'e','ret':'success'}, []), 
            'f':(node, 'myaction', {'time':1,'name':'f','ret':'success'}, ['d','e']), 
            }
    results = await event.run(commands, rollback=True, command_timeout=5, command_retry=3)
    print('Master: subevent with result:%s' % str(results))
    return { 'result':results, 'status':'success' }

secondary.start()
