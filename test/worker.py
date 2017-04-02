# -*- coding: utf-8 -*-

import sys
import asyncio
import random

sys.path.append("..")
import distgear

if len(sys.argv)==1:
    print('worker.py NAME')
    exit(1)
name = sys.argv[1]

worker = distgear.Worker(name, debug=False)

# define handlers for master
@worker.doAction('myaction')
async def testaction(paras):
    print('Worker: myaction with parameters: %s' % str(paras))
    time = paras['time']
    name = paras['name']
    ret = paras['ret']
    await asyncio.sleep(time)
    result = {'status':ret, 'result':'do command: '+name}
    print('Worker: myaction with result: %s' % str(result))
    return result

@worker.undoAction('myaction')
async def testundoaction(paras):
    print('Worker: undo myaction with parameters: %s' % str(paras))
    time = paras['time']
    name = paras['name']
    ret = paras['ret']
    await asyncio.sleep(time)
    result = {'status':ret, 'result':'undo command: '+name}
    print('Worker: undo myaction with result: %s' % str(result))
    return result



worker.start()
