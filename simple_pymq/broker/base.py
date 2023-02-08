import asyncio
from abc import ABC
from numbers import Number
from typing import Any, Optional, Text

from simple_pymq.exceptions import FullError, EmptyError


class Broker(ABC):
    def __init__(
        self,
        name: Text = "Broker",
        maxsize: int = 0,
        *args,
        block: bool = True,
        timeout: Optional[Number] = None,
        **kwargs
    ):
        self.name = name
        self.maxsize = maxsize
        self.block = block
        self.timeout = timeout

    async def empty(self) -> bool:
        raise NotImplementedError

    async def full(self) -> bool:
        raise NotImplementedError

    async def get(self, block: bool = True, timeout: Optional[Number] = None) -> Any:
        raise NotImplementedError

    async def get_nowait(self) -> Any:
        raise NotImplementedError

    async def join(self) -> None:
        raise NotImplementedError

    async def put(
        self, item: Any, block: bool = True, timeout: Optional[Number] = None
    ) -> None:
        raise NotImplementedError

    async def put_nowait(self, item: Any) -> None:
        raise NotImplementedError

    async def qsize(self) -> int:
        raise NotImplementedError

    async def task_done(self) -> None:
        raise NotImplementedError


class QueueBroker(Broker):
    def __init__(
        self,
        name: Text = "QueueBroker",
        maxsize: int = 0,
        block: bool = True,
        timeout: Optional[Number] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        *args,
        **kwargs
    ):
        super(QueueBroker, self).__init__(
            name=name, maxsize=maxsize, *args, block=block, timeout=timeout, **kwargs
        )
        self.name = name
        self.maxsize = maxsize
        self.queue = asyncio.Queue(maxsize=maxsize, loop=loop)

    async def empty(self) -> bool:
        return self.queue.empty()

    async def full(self) -> bool:
        return self.queue.full()

    async def get(
        self, block: Optional[bool] = None, timeout: Optional[Number] = None
    ) -> Any:
        block = block or self.block
        timeout = timeout or self.timeout

        if block is True:
            item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
        else:
            item = self.queue.get_nowait()

        return item

    async def get_nowait(self) -> Any:
        try:
            item = self.queue.get_nowait()
            return item
        except asyncio.QueueEmpty:
            raise EmptyError

    async def join(self) -> None:
        await self.queue.join()

    async def put(
        self, item: Any, block: bool = True, timeout: Optional[Number] = None
    ) -> None:
        block = block or self.block
        timeout = timeout or self.timeout
        if block is True:
            await asyncio.wait_for(self.queue.put(item), timeout=timeout)
        else:
            await self.queue.put_nowait(item)

    async def put_nowait(self, item: Any):
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            raise FullError

    async def qsize(self) -> int:
        return self.queue.qsize()

    async def task_done(self) -> None:
        self.queue.task_done()
