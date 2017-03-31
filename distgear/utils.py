# -*- coding: utf-8 -*-

__all__ = []

from json import dumps, loads, JSONDecodeError

async def zmq_send(socket, content, topic=None):
    if type(content) is not dict:
        return True
    if topic is None:
        await socket.send_multipart([str.encode(dumps(content))])
    else:
        await socket.send_multipart([str.encode(topic), str.encode(dumps(content))])
    return False

async def zmq_recv(socket, drop_topic=False):
    message = await socket.recv_multipart()
    message = [ bytes.decode(x) for x in message]
    ret = None
    try:
        if drop_topic and len(message) >= 2:
            ret = loads(message[1])
        else:
            ret = loads(message[0])
    except JSONDecodeError:
        ret = None
    return ret

    

