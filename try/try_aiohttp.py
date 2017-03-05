import asyncio
from aiohttp import web

async def handler(request):
    print(request.url)
    await asyncio.sleep(5)
    return web.Response(text="OK")


loop = asyncio.get_event_loop()
# call server() will create a Protocol
server = web.Server(handler)
# create_server will return a coroutine
coro = loop.create_server(server, '0.0.0.0', 8000)
# loop.run_until_complete will run coro to finish server creating
loop.run_until_complete(coro)
print("== Server is Created and begin to server ... ==")
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass
loop.close()
