import asyncio

import pytest

from mqflow.broker import SimpleFileBroker
from mqflow.broker.fs import SimpleFileLock
from mqflow.exceptions import FullError, EmptyError
from tests.config import settings as test_settings


test_id = str(test_settings.test_uuid).split("-")[0]


@pytest.mark.asyncio
async def test_simple_file_lock_operation():
    lock_path = "/tmp/test_simple_file_lock_operation_"
    lock_path += f"{test_id}.lock"

    # Create lock with quick lock timeout for testing.
    lock = SimpleFileLock(lock_path=lock_path, lock_timeout=0.5)

    # Test lock acquire context manager
    async with lock:
        # Do something
        pass

    # Worker 1 acquires lock manually
    lock_acquired = await lock.acquire(block=False)
    assert lock_acquired is True

    # Worker 2 acquires lock manually too
    lock_acquired = await lock.acquire(block=False)
    assert lock_acquired is False

    # Worker 3 wait lock timeout
    lock_acquired = await lock.acquire(block=True, timeout=1.0)
    assert lock_acquired is True


@pytest.mark.asyncio
async def test_simple_file_broker_basic_operation():
    broker_file = "/tmp/test_simple_file_broker_basic_operation_"
    broker_file += f"{test_id}.queue"

    q = SimpleFileBroker(file=broker_file, maxsize=2)

    assert await q.empty() is True

    await q.put(True)
    await q.put_nowait(True)

    assert await q.qsize() == 2
    assert await q.full() is True

    assert await q.get() is True
    assert await q.get_nowait() is True


@pytest.mark.asyncio
async def test_simple_file_broker_handle_operation_error():
    broker_file = "/tmp/test_simple_file_broker_handle_operation_error_"
    broker_file += f"{test_id}.queue"

    q = SimpleFileBroker(file=broker_file, maxsize=2)

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
    broker_file = "/tmp/test_queue_broker_handle_timeout_error_"
    broker_file += f"{test_id}.queue"

    q = SimpleFileBroker(file=broker_file, maxsize=2)

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
