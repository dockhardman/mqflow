import logging
import math
from abc import ABC
from typing import Any, Optional, Text, Type

from simple_pymq.broker.base import Broker
from simple_pymq.config import settings


logger = logging.getLogger(settings.logger_name)


class Consumer(ABC):
    def __init__(
        self,
        name: Text = "Consumer",
        *args,
        block: bool = True,
        timeout: Optional[float] = None,
        max_consume_count: int = 0,
        **kwargs,
    ):
        self.name = name or self.__class__.__name__
        self.max_consume_count = 0 if max_consume_count <= 0 else int(max_consume_count)
        self.block = block
        self.timeout = timeout

    async def listen(
        self,
        broker: Type[Broker],
        *args,
        block: Optional[bool] = None,
        timeout: Optional[float] = None,
        max_consume_count: Optional[int] = None,
        raise_error: bool = True,
        **kwargs,
    ):
        block = self.block if block is None else block
        timeout = self.timeout if timeout is None else timeout
        max_consume_count = (
            self.max_consume_count if max_consume_count is None else max_consume_count
        )
        max_consume_count = (
            float("inf")
            if max_consume_count <= 0 or math.isinf(max_consume_count)
            else math.ceil(max_consume_count)
        )

        consume_count = 0
        while True:
            item = await broker.get(block=block, timeout=timeout)

            try:
                await self.consume(item=item, broker=broker)

            except Exception as e:
                logger.exception(e)
                logger.error(f"Consumer '{self.name}' raise error: {str(e)}")
                if raise_error is True:
                    raise e

            finally:
                consume_count += 1

            if consume_count >= max_consume_count:
                break

    async def consume(self, item: Any):
        raise NotImplementedError


class NullConsumer(Consumer):
    def __init__(
        self,
        name: Text = "NullConsumer",
        *args,
        block: bool = True,
        timeout: Optional[float] = None,
        max_consume_count: int = 0,
        **kwargs,
    ):
        super(NullConsumer, self).__init__(
            name=name,
            *args,
            block=block,
            timeout=timeout,
            max_consume_count=max_consume_count,
            **kwargs,
        )

    async def consume(self, item: Any, broker: Type[Broker]):
        await broker.task_done()


class PrintConsumer(Consumer):
    def __init__(
        self,
        name: Text = "PrintConsumer",
        *args,
        block: bool = True,
        timeout: Optional[float] = None,
        max_consume_count: int = 0,
        **kwargs,
    ):
        super(PrintConsumer, self).__init__(
            name=name,
            *args,
            block=block,
            timeout=timeout,
            max_consume_count=max_consume_count,
            **kwargs,
        )

    async def consume(self, item: Any, broker: Type[Broker]):
        print(item)
        logger.info(item)
        await broker.task_done()
