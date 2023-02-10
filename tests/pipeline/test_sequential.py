import pytest

from simple_pymq import (
    PrintConsumer,
    QueueBroker,
    SimpleMessageQueue,
    TimeCounterProducer,
)


@pytest.mark.asyncio
async def test_simple_message_queue_basic_operation():
    q = QueueBroker(maxsize=50)
    c = PrintConsumer(max_consume_count=49)
    p = TimeCounterProducer(count_seconds=0.001, max_produce_count=50)
    mq = SimpleMessageQueue()

    await mq.run(broker=q, producers=p, consumers=c)
    assert await q.qsize() == 1
