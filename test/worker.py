# -*- coding: utf-8 -*-

import sys
import asyncio
import random

sys.path.append("../distgear")
from app import Worker

if len(sys.argv)==1:
    print('worker.py NAME')
    exit(1)
name = sys.argv[1]
worker = Worker(name)

# define handlers for master
@worker.doAction('myaction')
async def testaction(paras):
    print('<Worker.myaction> : '+str(paras))
    await asyncio.sleep(random.randint(0,1))
    if paras=='c':
        return {'status':'fail', 'result':'test fail'}
    return {'status':'success', 'result':'action done'}

@worker.undoAction('myaction')
async def testundoaction(paras):
    print('<Worker.undomyaction> : '+str(paras))
    await asyncio.sleep(random.randint(0,1))
    return {'status':'success', 'result':'undo action of '+str(paras)}


worker.start()
