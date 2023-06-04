from mqflow.broker import QueueBroker
from mqflow.consumer import Consumer
from mqflow.exceptions import EmptyError


def test_consumer():
    max_count = 3
    broker = QueueBroker(maxsize=max_count)
    for _ in range(max_count):
        broker.put(1)
    consumer = Consumer(
        target=(lambda *args, **kwargs: (print(args, kwargs))), max_count=max_count
    )
    consumer.listen(broker=broker)
    assert consumer.count == max_count


def test_consumer_exceptions():
    broker = QueueBroker()
    consumer = Consumer(target=(lambda *args, **kwargs: (print(args, kwargs))))

    try:
        consumer.listen(broker=broker, timeout=0.01)
        assert False
    except EmptyError:
        pass
