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
    time = random.randint(0,5)
    print('Worker: myaction need time : %s' % str(time))
    await asyncio.sleep(time)
    if paras=='d':
        result = {'status':'fail', 'result':'test fail'}
    else:
        result = {'status':'success', 'result':'action done'}
    print('Worker: myaction with result: %s' % str(result))
    return result

@worker.undoAction('myaction')
async def testundoaction(paras):
    print('Worker: undo myaction with parameters: %s' % str(paras))
    await asyncio.sleep(random.randint(0,2))
    result = {'status':'success', 'result':'undo action'}
    print('Worker: undo myaction with result: %s' % str(result))
    return result


worker.start()
