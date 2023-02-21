import pytest

from simple_pymq.broker import QueueBroker
from simple_pymq.producer import TimeCounterProducer
from simple_pymq.exceptions import FullError


@pytest.mark.asyncio
async def test_time_counter_producer_basic_operation():
    producer = TimeCounterProducer(count_seconds=0.001, max_produce_count=10)
    q = QueueBroker(maxsize=5)

    try:
        await producer.produce(
            broker=q, block=False, timeout=1.0, raise_full_error=True
        )
        assert False
    except FullError:
        assert True

    assert await q.full()
