from abc import ABC
from typing import Any, Text, Type

from simple_pymq.broker.base import Broker


class Consumer(ABC):
    def __init__(self, name: Text = "Consumer", *args, **kwargs):
        self.name = name

    async def listen(self, broker: Type[Broker], *args, **kwargs):
        while True:
            item = await broker.get()
            await self.consume(item=item)

    async def consume(self, item: Any):
        raise NotImplementedError
