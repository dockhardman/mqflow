import asyncio

import pytest
from redis.client import Redis

from simple_pymq.broker import RedisBroker
from simple_pymq.exceptions import FullError, EmptyError
from tests.config import console, logger, settings as test_settings


test_id = str(test_settings.test_uuid).split("-")[0]

is_redis_ready = False
try:
    r = Redis(
        host=test_settings.test_redis_host,
        port=test_settings.test_redis_port,
        db=test_settings.test_redis_db,
        username=test_settings.test_redis_username,
        password=test_settings.test_redis_password,
    )
    assert r.ping() is True
    is_redis_ready = True
    logger.debug("Redis is ready.")
except Exception as e:
    logger.warning("Redis is not ready, then use memory broker instead.")
    from simple_pymq.broker import QueueBroker as RedisBroker


@pytest.mark.asyncio
async def test_redis_broker_basic_operation():
    q = RedisBroker(
        host=test_settings.test_redis_host,
        port=test_settings.test_redis_port,
        db=test_settings.test_redis_db,
        username=test_settings.test_redis_username,
        password=test_settings.test_redis_password,
        key_base_name=f"{test_id}_test_redis_broker_basic_operation",
        key_expire=30,
        maxsize=2,
    )

    assert await q.qsize() is 0
    assert await q.empty() is True
    assert await q.full() is False

    await q.put(True)
    assert await q.qsize() is 1
    assert await q.empty() is False
    assert await q.full() is False

    await q.put_nowait(True)
    assert await q.qsize() is 2
    assert await q.empty() is False
    assert await q.full() is True

    assert await q.get() is True
    assert await q.qsize() is 1
    assert await q.empty() is False
    assert await q.full() is False

    assert await q.get_nowait() is True
    assert await q.qsize() is 0
    assert await q.empty() is True
    assert await q.full() is False


@pytest.mark.asyncio
async def test_redis_broker_no_maxsize():
    q = RedisBroker(
        host=test_settings.test_redis_host,
        port=test_settings.test_redis_port,
        db=test_settings.test_redis_db,
        username=test_settings.test_redis_username,
        password=test_settings.test_redis_password,
        key_base_name=f"{test_id}_test_redis_broker_no_maxsize",
        key_expire=30,
    )

    assert await q.qsize() is 0
    assert await q.empty() is True
    assert await q.full() is False

    for i in range(20):
        await q.put_nowait(True)
        assert await q.qsize() is (i + 1)
        assert await q.empty() is False
        assert await q.full() is False

    for i in range(20, 0 - 1):
        assert await q.empty() is False
        assert await q.full() is False
        assert await q.get_nowait() is True
        assert await q.qsize() is i - 1


@pytest.mark.asyncio
async def test_redis_broker_handle_operation_error():
    q = RedisBroker(
        host=test_settings.test_redis_host,
        port=test_settings.test_redis_port,
        db=test_settings.test_redis_db,
        username=test_settings.test_redis_username,
        password=test_settings.test_redis_password,
        key_base_name=f"{test_id}_test_redis_broker_handle_operation_error",
        key_expire=30,
        maxsize=2,
    )

    try:
        await q.get_nowait()
    except EmptyError:
        pass

    await q.put(True)
    await q.put(True)
    try:
        await q.put_nowait(True)
    except FullError:
        pass


@pytest.mark.asyncio
async def test_redis_broker_handle_timeout_error():
    q = RedisBroker(
        host=test_settings.test_redis_host,
        port=test_settings.test_redis_port,
        db=test_settings.test_redis_db,
        username=test_settings.test_redis_username,
        password=test_settings.test_redis_password,
        key_base_name=f"{test_id}_test_redis_broker_handle_timeout_error",
        key_expire=30,
        maxsize=2,
    )

    try:
        await q.get(block=True, timeout=0.01)
        assert False
    except asyncio.exceptions.TimeoutError:
        pass

    try:
        await q.put(True, block=True, timeout=1.1)
        await q.put(True, block=True, timeout=1.1)
        await q.put(True, block=True, timeout=1.1)
        assert False
    except asyncio.TimeoutError:
        pass
