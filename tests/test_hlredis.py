# -*- coding: utf-8 -*-
#
#        H A P P Y    H A C K I N G !
#              _____               ______
#     ____====  ]OO|_n_n__][.      |    |
#    [________]_|__|________)<     |YANG|
#     oo    oo  'oo OOOO-| oo\\_   ~o~~o~
# +--+--+--+--+--+--+--+--+--+--+--+--+--+
#               Jianing Yang @ 16 Feb, 2018
#
from tornado_battery.hlredis import register_redis_options
from tornado_battery.hlredis import RedisConnector, RedisConnectorError
import pytest

pytestmark = pytest.mark.asyncio
register_redis_options("test", "redis://172.17.0.2/0")


@pytest.fixture
async def redis():
    redis = RedisConnector.instance("test")
    await redis.connect()
    return redis.connection()


async def test_set(redis):
    value = await redis.set("hlredis_decorator_value", "1986")
    assert value == True


async def test_get(redis):
    value = await redis.get("hlredis_decorator_value")
    assert value == "1986"


async def test_no_connection():
    match = r"^no connection of noconn found$"
    with pytest.raises(RedisConnectorError, match=match):
        RedisConnector.instance("noconn").connection()


async def test_invalid_connection_scheme():
    from tornado.options import options
    options.redis_test_uri = "test://"
    match = r" is not a redis connection scheme$"
    with pytest.raises(RedisConnectorError, match=match):
        await RedisConnector.instance("test").connect()


async def test_option_name():
    from tornado_battery.redis import option_name

    assert option_name("master", "uri") == "redis-master-uri"
    assert option_name("slave", "uri") == "redis-slave-uri"
