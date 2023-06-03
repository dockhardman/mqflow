from abc import ABC
from multiprocessing import Queue as MPQueue
from numbers import Number
from queue import Queue, Empty as QueueEmpty, Full as QueueFull
from typing import Generic, Optional, Text, TypeVar

from mqflow.exceptions import FullError, EmptyError


T = TypeVar("T")


class BrokerBase(ABC, Generic[T]):
    def __init__(
        self,
        maxsize: int = 0,
        *args,
        name: Text = "BrokerBase",
        block: bool = True,
        timeout: Optional[Number] = None,
        **kwargs,
    ):
        self.name = name
        self.maxsize = maxsize
        self.block = block
        self.timeout = timeout

    def __repr__(self) -> Text:
        return f"{self.__class__.__name__}(name={self.name}, maxsize={self.maxsize})"

    def __str__(self) -> Text:
        return self.__repr__()

    def __len__(self) -> int:
        return self.qsize()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.get_nowait()
        except EmptyError:
            raise StopIteration

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def empty(self) -> bool:
        raise NotImplementedError

    def full(self) -> bool:
        raise NotImplementedError

    def get(self, block: Optional[bool] = None, timeout: Optional[Number] = None) -> T:
        raise NotImplementedError

    def get_nowait(self) -> T:
        raise NotImplementedError

    def join(self) -> None:
        raise NotImplementedError

    def put(
        self, item: T, block: Optional[bool] = None, timeout: Optional[Number] = None
    ) -> None:
        raise NotImplementedError

    def put_nowait(self, item: T) -> None:
        raise NotImplementedError

    def qsize(self) -> int:
        raise NotImplementedError

    def task_done(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        pass


class QueueBroker(BrokerBase[T]):
    def __init__(
        self,
        maxsize: int = 0,
        *args,
        name: Text = "QueueBroker",
        block: bool = True,
        timeout: Optional[Number] = None,
        queue: Optional["Queue[T]"] = None,
        **kwargs,
    ):
        super().__init__(
            maxsize, *args, name=name, block=block, timeout=timeout, kwargs=kwargs
        )

        self.queue = queue or Queue(maxsize=maxsize)
        self.maxsize = self.queue.maxsize

    def empty(self) -> bool:
        return self.queue.empty()

    def full(self) -> bool:
        return self.queue.full()

    def get(self, block: Optional[bool] = None, timeout: Optional[Number] = None) -> T:
        block = self.block if block is None else block
        timeout = self.timeout if timeout is None else timeout

        try:
            return self.queue.get(block=block, timeout=timeout)
        except QueueEmpty as e:
            raise EmptyError(e)
        except Exception as e:
            raise e

    def get_nowait(self) -> T:
        try:
            return self.queue.get_nowait()
        except QueueEmpty as e:
            raise EmptyError(e)
        except Exception as e:
            raise e

    def join(self) -> None:
        self.queue.join()

    def put(
        self, item: T, block: Optional[bool] = None, timeout: Optional[Number] = None
    ):
        block = self.block if block is None else block
        timeout = self.timeout if timeout is None else timeout

        try:
            self.queue.put(item, block=block, timeout=timeout)
        except QueueFull as e:
            raise FullError(e)
        except Exception as e:
            raise e

    def put_nowait(self, item: T) -> None:
        try:
            self.queue.put_nowait(item)
        except QueueFull as e:
            raise FullError(e)
        except Exception as e:
            raise e

    def qsize(self) -> int:
        return self.queue.qsize()

    def task_done(self) -> None:
        self.queue.task_done()

    def close(self) -> None:
        pass


class MPQueueBroker(QueueBroker[T]):
    def __init__(
        self,
        maxsize: int = 0,
        *args,
        name: Text = "MultipleProcessingBroker",
        block: bool = True,
        timeout: Optional[Number] = None,
        queue: Optional["MPQueue[T]"] = None,
        **kwargs,
    ):
        super().__init__(
            maxsize, *args, name=name, block=block, timeout=timeout, kwargs=kwargs
        )

        self.queue = queue or MPQueue(maxsize=maxsize)
        self.maxsize = self.queue._maxsize

    def close(self) -> None:
        self.queue.close()
        self.queue.join_thread()
