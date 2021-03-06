#!/usr/bin/env python
__author__ = 'ziyan.yin'

import asyncio
import logging
import os
import pickle
from asyncio import QueueEmpty
from nado_utils import cryptutils

from . import utils, AioUnit

BUF_SIZE = 1024
MAX_SIZE = 2**20 * 5
HOST = ''
PORT = 11211

white_list = [
    '127.0.0.1'
]
data_format = {
    'success': True,
    'data': '',
    'message': '',
    'code': 0,
}
logger = logging.getLogger('unit')


try:
    import uvloop as loop_policy
    asyncio.set_event_loop_policy(loop_policy.EventLoopPolicy())
except ImportError:
    loop_policy = None

loop = asyncio.get_event_loop()

_queue = asyncio.PriorityQueue()


class ParamsError(Exception):
    pass


class UnknownServiceError(Exception):
    pass


class OutOfBoundError(Exception):
    pass


def __get_unit(command):
    return utils.get_unit(command['service'], command['method'])


def __struct(data):
    return memoryview(pickle.dumps(data, protocol=5))


def __create_task(message):
    command = pickle.loads(message, encoding='utf-8')
    if 'signature' not in command:
        raise ParamsError()
    else:
        signature = command['signature']
        del command['signature']
        if signature != cryptutils.sha256(str(command) + 'NadoUnit'):
            raise ParamsError()

    if 'service' in command and 'method' in command:
        ret, task = __get_unit(command)
        if ret:
            param = command['param'] if 'param' in command else {'action': 'view'}
            return task(param, operator=command['operator'], ip=command['ip'])
        else:
            raise UnknownServiceError(task)
    else:
        raise ParamsError()


async def consume():
    while True:
        task, writer = await _queue.get()
        await work_coroutine(writer, task)
        _queue.task_done()


async def work_coroutine(writer, task):
    res = await work(task)
    writer.write(__struct(res))
    await writer.drain()
    writer.close()


async def work(instance):
    if issubclass(type(instance), AioUnit):
        return await instance.execute()
    else:
        def __work(task):
            return task.execute()
        return await loop.run_in_executor(None, __work, instance)


async def produce(instance, writer):
    await _queue.put((instance, writer))


async def handle(reader, writer):
    try:
        content_length = int((await reader.read(16)).removesuffix(b'\r\n\r\n'))
        if content_length > MAX_SIZE:
            raise OutOfBoundError()
        message = await reader.read(content_length)
        instance = __create_task(message)
        logger.info('recv task %s' % instance.__class__.__name__)
        if instance.level <= 1:
            await work_coroutine(writer, instance)
        else:
            await produce(instance, writer)
    except UnknownServiceError as ex:
        res = data_format.copy()
        res['success'] = False
        res['message'] = '[10001]%s' % ex
        writer.write(__struct(res))
        await writer.drain()
        writer.close()
    except ParamsError:
        res = data_format.copy()
        res['success'] = False
        res['message'] = '[10004]'
        writer.write(__struct(res))
        await writer.drain()
        writer.close()
    except OutOfBoundError:
        res = data_format.copy()
        res['success'] = False
        res['message'] = '[10005]out of bounds'
        writer.write(__struct(res))
        await writer.drain()
        writer.close()


def main(port, initial=None):
    thread_count = min(32, (os.cpu_count() or 1) + 4)

    for i in range(thread_count):
        logger.info(f'Consumer {i + 1} started')
        asyncio.ensure_future(consume())

    coro = asyncio.start_server(handle, '', port)
    if initial:
        loop.run_until_complete(initial())
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    logger.info('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        # Close the server
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
        raise
