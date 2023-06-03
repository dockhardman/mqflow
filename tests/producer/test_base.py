from queue import Queue

from mqflow.producer import LoopingProducer, Producer


def test_producer():
    sum_args = (1, 2, 3)
    producer = Producer(Queue(), target=lambda *args: sum(args), args=sum_args)
    producer.produce()
    assert producer.count == 1
    assert producer.broker.get() == sum(sum_args)


def test_looping_producer():
    max_count = 3
    sum_args = (1, 2, 3)
    producer = LoopingProducer(
        Queue(), target=lambda *args: sum(args), args=sum_args, max_count=max_count
    )
    producer.produce()
    assert producer.count == max_count
    for _ in range(producer.broker.qsize()):
        assert producer.broker.get() == sum(sum_args)
