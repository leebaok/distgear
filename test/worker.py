# -*- coding: utf-8 -*-

import sys
import asyncio
import random

sys.path.append("..")
from distgear.app import Worker

if len(sys.argv)==1:
    print('worker.py NAME')
    exit(1)
name = sys.argv[1]
worker = Worker(name, '127.0.0.1')

# define handlers for master
@worker.doAction('myaction')
async def testaction(paras):
    print('<Worker.myaction> : '+str(paras))
    await asyncio.sleep(random.randint(1,10))
    return {'status':'success', 'result':'action done'}

worker.start()
