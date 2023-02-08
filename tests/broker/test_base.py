import pytest

from simple_pymq import QueueBroker


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
