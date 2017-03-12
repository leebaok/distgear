# -*- coding: utf-8 -*-

import sys

sys.path.append("..")
from distgear.app import Worker

worker = Worker('w1', '127.0.0.1')

# define handlers for master
@worker.register('test')
async def testaction(paras):
    print('<worker action>')
    print(paras)
    return 'action done'

worker.start()
