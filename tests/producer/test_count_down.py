from queue import Queue
from threading import Thread

from rich import print

from mqflow.producer.count_down import CountDownProducer


def test_count_down():
    max_count = 3
    producer = CountDownProducer(Queue(), max_count=max_count)
    producer.produce()
    assert producer.count == max_count


def test_count_down_stop():
    try:
        producer = CountDownProducer(Queue())
        t1 = Thread(target=producer.produce)
        t2 = Thread(target=producer.stop)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        producer.stop()
        print("User KeyboardInterrupt")
