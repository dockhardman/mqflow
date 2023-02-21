import asyncio

import pytest

from simple_pymq.broker import QueueBroker
from simple_pymq.consumer import PrintConsumer


@pytest.mark.parametrize("item_count,consume_count", [(4, 2), (2, 2)])
@pytest.mark.asyncio
async def test_consumer_basic_operation(item_count: int, consume_count: int):
    q = QueueBroker(maxsize=10)
    c = PrintConsumer()

    task = asyncio.create_task(c.listen(broker=q, max_consume_count=consume_count))

    for _ in range(item_count):
        await q.put_nowait(True)
    await task

    # Check consumer has consumed item.
    if item_count > consume_count:
        assert await q.qsize() == (item_count - consume_count)
    else:
        assert await q.qsize() == 0
