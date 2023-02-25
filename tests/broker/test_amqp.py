import pytest

from simple_pymq.broker import AmqpBroker
from simple_pymq.exceptions import FullError, EmptyError
from tests.config import logger, settings as test_settings


test_id = str(test_settings.test_uuid).split("-")[0]

is_amqp_ready = False
if test_settings.test_amqp_url is not None:
    r = AmqpBroker(
        amqp_url=test_settings.test_amqp_url, amqp_queue_name="test", passive=False
    )
    r.amqp_connection.sleep(0.1)
    is_amqp_ready = True
    logger.debug("AMQP is ready.")
else:
    logger.warning("AMQP url is not applied, then use memory broker instead.")
    from simple_pymq.broker import QueueBroker as AmqpBroker


@pytest.mark.asyncio
async def test_amqp_broker_basic_operation():
    queue_name = f"test_amqp_broker_basic_operation_{test_id}"
    q = AmqpBroker(
        amqp_url=test_settings.test_amqp_url,
        amqp_queue_name=queue_name,
        passive=False,
        maxsize=2,
    )

    assert await q.qsize() == 0
    assert await q.empty() is True
    assert await q.full() is False

    await q.put(True)
    assert await q.qsize() == 1
    assert await q.empty() is False
    assert await q.full() is False

    await q.put(True)
    assert await q.qsize() == 2
    assert await q.empty() is False
    assert await q.full() is True

    try:
        await q.put_nowait(True)
    except FullError:
        pass

    assert await q.get() is True
    assert await q.qsize() == 1
    assert await q.empty() is False
    assert await q.full() is False

    assert await q.get() is True
    assert await q.qsize() == 0
    assert await q.empty() is True
    assert await q.full() is False

    try:
        assert await q.get_nowait() is True
    except EmptyError:
        pass

    q.amqp_channel.queue_delete(queue_name)
    await q.close()
