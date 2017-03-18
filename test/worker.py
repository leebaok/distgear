# -*- coding: utf-8 -*-

import sys
import asyncio

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
    print('<worker action>')
    print(paras)
    await asyncio.sleep(3)
    return {'status':'success', 'result':'action done'}

worker.start()
