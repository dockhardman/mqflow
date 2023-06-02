import asyncio

import pytest

from mqflow.broker import QueueBroker
from mqflow.exceptions import FullError, EmptyError


@pytest.mark.asyncio
async def test_queue_broker_basic_operation():
    q = QueueBroker(maxsize=2)
    assert await q.empty() is True

    await q.put(True)
    await q.put_nowait(True)

    assert await q.qsize() == 2
    assert await q.full() is True

    await q.get()
    await q.get_nowait()


@pytest.mark.asyncio
async def test_queue_broker_handle_operation_error():
    q = QueueBroker(maxsize=2)

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
async def test_queue_broker_handle_timeout_error():
    q = QueueBroker(maxsize=2)

    try:
        await q.get(block=True, timeout=0.001)
        assert False
    except asyncio.exceptions.TimeoutError:
        pass

    try:
        await q.put(True, block=True, timeout=0.001)
        await q.put(True, block=True, timeout=0.001)
        await q.put(True, block=True, timeout=0.001)
        assert False
    except asyncio.TimeoutError:
        pass
