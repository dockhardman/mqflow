from abc import ABC
from numbers import Number
from typing import Any, Optional, Text


class Broker(ABC):
    def __init__(self, name: Text = "Broker", *args, **kwargs):
        self.name = name

    async def empty(self):
        raise NotImplementedError

    async def full(self):
        raise NotImplementedError

    async def get(self, block: bool = True, timeout: Optional[Number] = None) -> Any:
        raise NotImplementedError

    async def get_nowait(self) -> Any:
        raise NotImplementedError

    async def join(self):
        raise NotImplementedError

    async def put(
        self, item: Any, block: bool = True, timeout: Optional[Number] = None
    ):
        raise NotImplementedError

    async def put_nowait(self, item: Any):
        raise NotImplementedError

    async def qsize(self):
        raise NotImplementedError

    async def task_done(self):
        raise NotImplementedError
