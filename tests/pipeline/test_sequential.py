from threading import Thread
import time

from mqflow.broker import QueueBroker
from mqflow.consumer import Consumer
from mqflow.pipeline import SequentialMessageQueue
from mqflow.producer import Producer


def test_sequential_message_queue():
    max_count = 3
    producer = Producer(
        name="test_producer", target=(lambda *args, **kwargs: True), max_count=max_count
    )
    consumer = Consumer(name="test_consumer", target=print, max_count=max_count)
    broker = QueueBroker()
    mq = SequentialMessageQueue(
        producers=[producer], consumers=[consumer], broker=broker
    )
    mq.run()


def test_sequential_message_queue_stop():
    def delay_stop(mq: "SequentialMessageQueue", sleep: float):
        time.sleep(sleep)
        mq.stop()

    producer = Producer(
        name="test_producer", target=(lambda *args, **kwargs: True), interval_seconds=1
    )
    consumer = Consumer(name="test_consumer", target=print)
    broker = QueueBroker()
    mq = SequentialMessageQueue(
        producers=[producer], consumers=[consumer], broker=broker
    )

    stop_signal = Thread(target=delay_stop, kwargs=dict(mq=mq, sleep=2))
    stop_signal.start()

    mq.run()
