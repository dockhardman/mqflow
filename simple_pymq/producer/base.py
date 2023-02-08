from abc import ABC
from typing import Text, Type

from simple_pymq.broker.base import Broker


class Producer(ABC):
    def __init__(self, name: Text = "Producer", *args, **kwargs):
        self.name = name

    async def produce(self, broker: Type[Broker], *args, **kwargs):
        raise NotImplementedError
