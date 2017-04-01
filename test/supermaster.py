# -*- coding: utf-8 -*-

import sys
import random

sys.path.append("..")
import distgear

supermaster = distgear.SuperMaster('supermaster', debug=False)

# define handlers for master
@supermaster.handleEvent('myevent')
async def testevent(event, master):
    print('SuperMaster: do myevent')
    nodes = list(master.nodeinfo.keys())
    if len(nodes)==0:
        return {'result':'no workers', 'status':'fail'}
    node = nodes[random.randint(0, len(nodes)-1)]
    result = await event.run_command((node, 'subevent', 'Nothing'))
    print('SuperMaster: result:%s' % str(result))
    return result

supermaster.start()
