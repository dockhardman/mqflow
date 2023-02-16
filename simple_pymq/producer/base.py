import asyncio
import logging
from abc import ABC
from numbers import Number
from typing import Any, Optional, Text, Type

from simple_pymq.broker.base import Broker
from simple_pymq.config import settings
from simple_pymq.exceptions import FullError


logger = logging.getLogger(settings.logger_name)


class Producer(ABC):
    def __init__(
        self,
        name: Text = "Producer",
        *args,
        block: bool = True,
        timeout: Optional[Number] = None,
        **kwargs,
    ):
        self.name = name
        self.timeout = timeout
        self.block = block

    async def produce(
        self,
        broker: Type[Broker],
        *args,
        block: Optional[bool] = None,
        timeout: Optional[Number] = None,
        **kwargs,
    ):
        raise NotImplementedError


class TimeCounterProducer(Producer):
    def __init__(
        self,
        name: Text = "TimeCounterProducer",
        block: bool = True,
        timeout: Optional[Number] = None,
        count_seconds: float = 1.0,
        max_produce_count: Optional[int] = None,
        put_value: Any = 1,
        *args,
        **kwargs,
    ):
        super(TimeCounterProducer, self).__init__(
            name=name, *args, block=block, timeout=timeout, **kwargs
        )
        self.count_seconds = count_seconds
        self.max_produce_count = max_produce_count
        self.put_value = put_value

    async def produce(
        self,
        broker: Type[Broker],
        *args,
        block: Optional[bool] = None,
        timeout: Optional[Number] = None,
        count_seconds: Optional[float] = None,
        max_produce_count: Optional[int] = None,
        put_value: Any = None,
        ignore_full_error: bool = False,
        raise_full_error: bool = False,
        **kwargs,
    ):
        timeout = self.timeout if timeout is None else timeout
        block = self.block if block is None else block
        count_seconds = self.count_seconds if count_seconds is None else count_seconds
        max_produce_count = max_produce_count or self.max_produce_count or float("inf")
        put_value = self.put_value if put_value is None else put_value

        count = 0
        while True:
            try:
                await broker.put(put_value, block=block, timeout=timeout)

            except FullError as e:
                if raise_full_error is True:
                    raise e
                if ignore_full_error is True:
                    logger.info(
                        f"The broker '{broker}' is full, "
                        + f"so skip the item '{put_value}'."
                    )
                else:
                    logger.error(
                        f"The broker '{broker}' is full, "
                        + f"so skip the item '{put_value}'."
                    )

            finally:
                count += 1

            if count >= max_produce_count:
                break

            await asyncio.sleep(count_seconds)
