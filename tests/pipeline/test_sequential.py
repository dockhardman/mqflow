import pytest

from mqflow.consumer import NullConsumer, NullConsumer
from mqflow.broker import QueueBroker, RedisBroker, SimpleFileBroker
from mqflow.producer import TimeCounterProducer
from mqflow.pipeline import SimpleMessageQueue
from tests.config import settings as test_settings


test_id = str(test_settings.test_uuid).split("-")[0]


@pytest.mark.asyncio
async def test_simple_message_queue_basic_operation():
    q = QueueBroker(maxsize=50)
    c = NullConsumer(max_consume_count=49)
    p = TimeCounterProducer(count_seconds=0.001, max_produce_count=50)
    mq = SimpleMessageQueue()

    await mq.run(broker=q, producers=p, consumers=c)
    assert await q.qsize() == 1


@pytest.mark.asyncio
async def test_simple_message_queue_massive_tasks():
    total_tasks = 10000
    consumer_count = 10
    producer_count = 10

    q = QueueBroker(maxsize=128)
    consumers = [
        NullConsumer(max_consume_count=total_tasks // consumer_count)
        for _ in range(consumer_count)
    ]
    producers = [
        TimeCounterProducer(
            count_seconds=0.0, max_produce_count=total_tasks // producer_count
        )
        for _ in range(producer_count)
    ]
    mq = SimpleMessageQueue()

    await mq.run(broker=q, producers=producers, consumers=consumers)
    assert await q.qsize() == 0


@pytest.mark.asyncio
async def test_simple_message_queue_of_simple_file_broker():
    total_tasks = 1000  # The performance is not good now.
    consumer_count = 10
    producer_count = 10

    broker_file = "/tmp/test_simple_message_queue_of_simple_file_broker_"
    broker_file += f"{test_id}.queue"

    q = SimpleFileBroker(file=broker_file, maxsize=128)
    consumers = [
        NullConsumer(max_consume_count=total_tasks // consumer_count)
        for _ in range(consumer_count)
    ]
    producers = [
        TimeCounterProducer(
            count_seconds=0.0, max_produce_count=total_tasks // producer_count
        )
        for _ in range(producer_count)
    ]
    mq = SimpleMessageQueue()

    await mq.run(broker=q, producers=producers, consumers=consumers)
    assert await q.qsize() == 0


@pytest.mark.asyncio
async def test_simple_message_queue_of_redis_broker():
    total_tasks = 300
    consumer_count = 10
    producer_count = 10

    q = RedisBroker(
        host=test_settings.test_redis_host,
        port=test_settings.test_redis_port,
        db=test_settings.test_redis_db,
        username=test_settings.test_redis_username,
        password=test_settings.test_redis_password,
        key_base_name=f"{test_id}_test_simple_message_queue_of_redis_broker",
        key_expire=30,
        maxsize=64,
    )
    consumers = [
        NullConsumer(max_consume_count=total_tasks // consumer_count)
        for _ in range(consumer_count)
    ]
    producers = [
        TimeCounterProducer(
            count_seconds=0.01, max_produce_count=total_tasks // producer_count
        )
        for _ in range(producer_count)
    ]
    mq = SimpleMessageQueue()

    await mq.run(broker=q, producers=producers, consumers=consumers)
    assert await q.qsize() == 0

    await q.close()
