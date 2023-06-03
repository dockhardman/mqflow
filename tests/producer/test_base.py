from queue import Queue

from mqflow.producer import Producer


def test_producer():
    sum_args = (1, 2, 3)
    producer = Producer(Queue(), target=lambda *args: sum(args), args=sum_args)
    producer.produce()
    assert producer.count == 1
    assert producer.broker.get() == sum(sum_args)
