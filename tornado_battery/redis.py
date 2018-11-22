# -*- coding: utf-8 -*-
#
#        H A P P Y    H A C K I N G !
#              _____               ______
#     ____====  ]OO|_n_n__][.      |    |
#    [________]_|__|________)<     |YANG|
#     oo    oo  'oo OOOO-| oo\\_   ~o~~o~
# +--+--+--+--+--+--+--+--+--+--+--+--+--+
#               Jianing Yang @ 12 Feb, 2018
#
from .exception import ServerException
from .pattern import NamedSingletonMixin, SingletonMixin
from . import TaskLocals
from tornado.options import define, options
from urllib.parse import urlparse
from contextlib import contextmanager

import aioredis
import asyncio
import functools
import logging

LOG = logging.getLogger('tornado.application')


class RedisConnectorError(ServerException):
    pass


class RedisConnector(NamedSingletonMixin):

    def __init__(self, name: str):
        self.name = name

    def connection(self):
        if not hasattr(self, '_connections') or not self._connections:
            raise RedisConnectorError(f'no connection of {self.name} found')
        return self._connections.get()

    async def connect(self, event_loop=None):
        name = self.name
        opts = options.group_dict(f'{name} redis')
        connection_string = opts[option_name(name, 'uri')]
        r = urlparse(connection_string)
        if r.scheme.lower() != 'redis':
            raise RedisConnectorError(
                f'{connection_string} is not a redis connection scheme')
        num_connections = opts[option_name(name, 'num-connections')]
        LOG.info(f'connecting redis [{self.name}] {connection_string}')
        if event_loop is None:
            event_loop = asyncio.get_event_loop()
        self._connections = await aioredis.create_pool(
            connection_string,
            encoding='UTF-8',
            minsize=int(num_connections[0]),
            maxsize=int(num_connections[-1]),
            loop=event_loop,
        )


class RedisConnectorManager(SingletonMixin):

    local_key = 'tornado_battery.redis.manager'

    def __init__(self):
        self.task_locals = TaskLocals(loop=None)

    @contextmanager
    def use(self, name):
        conn_stack = self.task_locals.get(self.local_key, None)
        if conn_stack is None:
            conn_stack = []
            self.task_locals.set(self.local_key, conn_stack)
        conn_stack.append(RedisConnector.instance(name).connection())
        try:
            yield
        finally:
            conn_stack.pop()

    def get(self):
        conn_stack = self.task_locals.get(self.local_key, None)
        assert conn_stack is not None, 'no redis manager is in use'
        return conn_stack[-1]


def option_name(instance: str, option: str) -> str:
    return f'redis-{instance}-{option}'


def register_redis_options(instance: str='master',
                           default_uri: str='redis://'):
    define(option_name(instance, 'uri'),
           default=default_uri,
           group=f'{instance} redis',
           help=f'redis connection uri for {instance}')
    define(option_name(instance, 'num-connections'), multiple=True,
           default=[1, 2],
           group=f'{instance} redis',
           help=f'# of redis connections for {instance}')


def use_redis(name: str):

    def wrapper(function):

        @functools.wraps(function)
        async def f(*args, **kwargs):
            with RedisConnectorManager.instance().use(name):
                retval = await function(*args, **kwargs)
            return retval
        return f

    return wrapper


def with_redis(name: str):

    def wrapper(function):

        @functools.wraps(function)
        async def f(*args, **kwargs):
            async with RedisConnector.instance(name).connection() as redis:
                if 'redis' in kwargs:
                    raise RedisConnectorError(
                        f'duplicated database argument for redis {name}')
                kwargs.update({'redis': aioredis.Redis(redis)})
                retval = await function(*args, **kwargs)
                return retval
        return f

    return wrapper


def with_redis_conn(function):

    @functools.wraps(function)
    async def f(*args, **kwargs):
        async with RedisConnectorManager.instance().get() as redis:
            if 'redis' in kwargs:
                raise RedisConnectorError(
                    f'duplicated database argument for redis')
            kwargs.update({'redis': aioredis.Redis(redis)})
            retval = await function(*args, **kwargs)
            return retval
    return f


def connect_redis(name: str):
    return RedisConnector.instance(name).connect
