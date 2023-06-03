from queue import Queue
from threading import Thread

from rich import print

from mqflow.producer.timer import LoopingTimerProducer, TimerProducer


def test_timer_producer():
    producer = TimerProducer(Queue(), timer_seconds=0.0)
    producer.produce()
    assert producer.count == 1


def test_looping_timer():
    max_count = 3
    producer = LoopingTimerProducer(Queue(), max_count=max_count)
    producer.produce()
    assert producer.count == max_count


def test_looping_timer_stop():
    try:
        producer = LoopingTimerProducer(Queue())
        t1 = Thread(target=producer.produce)
        t2 = Thread(target=producer.stop)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        producer.stop()
        print("User KeyboardInterrupt")
