import logging
import math
from abc import ABC
from typing import Any, Optional, Text, Type

from simple_pymq.broker.base import Broker
from simple_pymq.config import settings


logger = logging.getLogger(settings.logger_name)


class Consumer(ABC):
    def __init__(
        self, name: Text = "Consumer", *args, max_consume_count: int = 0, **kwargs
    ):
        self.name = name or self.__class__.__name__
        self.max_consume_count = 0 if max_consume_count <= 0 else int(max_consume_count)

    async def listen(
        self, broker: Type[Broker], *args, raise_error: bool = True, **kwargs
    ):
        while True:
            item = await broker.get()
            await self.consume(item=item)

    async def consume(self, item: Any):
        raise NotImplementedError


class PrintConsumer(Consumer):
    def __init__(
        self, name: Text = "PrintConsumer", *args, max_consume_count: int = 0, **kwargs
    ):
        super(PrintConsumer, self).__init__(
            name=name, *args, max_consume_count=max_consume_count, **kwargs
        )

    async def listen(
        self,
        broker: Type[Broker],
        *args,
        max_consume_count: Optional[int] = None,
        raise_error: bool = True,
        **kwargs,
    ):
        if max_consume_count is None:
            max_consume_count = self.max_consume_count
        if max_consume_count <= 0 or math.isinf(max_consume_count):
            max_consume_count = float("inf")
        else:
            max_consume_count = math.ceil(max_consume_count)

        consume_count = 0
        while True:
            item = await broker.get()

            try:
                await self.consume(item=item, broker=broker)

            except Exception as e:
                if raise_error is True:
                    raise e
                else:
                    logger.exception(e)
                    logger.error(f"Consumer '{self.name}' raise error: {str(e)}")

            consume_count += 1
            if consume_count >= max_consume_count:
                break

    async def consume(self, item: Any, broker: Type[Broker]):
        print(item)
        logger.info(item)
        await broker.task_done()
