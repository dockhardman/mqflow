from mqflow.broker import QueueBroker
from mqflow.producer import Producer
from mqflow.exceptions import FullError


def test_producer():
    max_count = 1
    sum_args = (1, 2, 3)
    broker = QueueBroker()
    producer = Producer(
        target=lambda *args: sum(args), args=sum_args, max_count=max_count
    )
    producer.publish(broker)
    assert producer.count == max_count
    assert broker.get() == sum(sum_args)


def test_producer_exceptions():
    max_count = 1
    sum_args = (1, 2, 3)
    broker = QueueBroker(maxsize=max_count)
    producer = Producer(target=lambda *args: sum(args), args=sum_args, timeout=0.01)

    try:
        producer.publish(broker)
        assert False
    except FullError:
        assert producer.count == max_count
        assert broker.get() == sum(sum_args)
