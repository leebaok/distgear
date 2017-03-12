# -*- coding: utf-8 -*-
"""
    try_asyncio.py
    ~~~~~~~~~~~~~~
    just try asyncio ( coroutine and asyncio event loop )

    Author: Bao Li
"""

import asyncio

async def coro1():
    print('coro 1')
    await asyncio.sleep(2)

async def coro2():
    print('coro 2')
    await asyncio.sleep(5)

async def wait():
    task1 = asyncio.ensure_future(coro1())
    task2 = asyncio.ensure_future(coro2())
    done, pending = await asyncio.wait([task1, task2])
    print(done)
    print('----')
    print(pending)


loop = asyncio.get_event_loop()
loop.run_until_complete(wait())
loop.close()
