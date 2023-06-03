from multiprocessing.queues import Queue
from queue import Queue

from mqflow.broker import MPQueueBroker, QueueBroker
from mqflow.exceptions import EmptyError, FullError


def test_queue_broker():
    value = 1
    broker = QueueBroker()
    broker.put(value)
    assert broker.get() == value


def test_queue_broker_exceptions():
    maxsize = 1
    broker = QueueBroker(queue=Queue(maxsize=maxsize))

    try:
        broker.get_nowait()
        assert False
    except EmptyError:
        pass

    try:
        broker.put_nowait(1)
        broker.put_nowait(2)
        assert False
    except FullError:
        pass

    try:
        broker.put(3, timeout=0.01)
        assert False
    except FullError:
        pass

    try:
        broker.get(timeout=0.01)
        broker.get(timeout=0.01)
        assert False
    except EmptyError:
        pass


def test_mp_queue_broker():
    with MPQueueBroker(maxsize=1) as broker:
        try:
            broker.get_nowait()
            assert False
        except EmptyError:
            pass

        try:
            broker.put_nowait(1)
            broker.put_nowait(2)
            assert False
        except FullError:
            pass

        try:
            broker.put(3, timeout=0.01)
            assert False
        except FullError:
            pass

        try:
            broker.get(timeout=0.01)
            broker.get(timeout=0.01)
            assert False
        except EmptyError:
            pass
