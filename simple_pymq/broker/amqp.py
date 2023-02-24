import asyncio
import pickle
from numbers import Number
from typing import Any, Dict, Optional, Text, Tuple, TypeVar

from pyassorted.asyncio.executor import run_func
from yarl import URL

from simple_pymq.broker.base import Broker
from simple_pymq.config import logger
from simple_pymq.exceptions import FullError, EmptyError


is_pika_installed = True
try:
    import pika
    from pika.frame import Header, Method
    from pika.exceptions import ChannelClosedByBroker
except ImportError:
    is_pika_installed = False


MESSAGE_PACK = TypeVar("MESSAGE_PACK", bound=Tuple["Method", "Header", bytes])


class AmqpBroker(Broker):
    def __init__(
        self,
        name: Text = "AmqpBroker",
        maxsize: int = 0,
        *args,
        amqp_url: Text,
        amqp_query: Optional[Dict] = None,
        amqp_queue_name: Optional[Text] = None,
        passive: bool = True,
        block: bool = True,
        timeout: Optional[Number] = None,
        **kwargs,
    ):
        if is_pika_installed is False:
            raise ImportError("Package 'pika' is not installed.")

        super(AmqpBroker, self).__init__(
            name=name, maxsize=maxsize, *args, block=block, timeout=timeout, **kwargs
        )

        self.amqp_url = URL(amqp_url)
        if amqp_query is not None:
            self.amqp_url = self.amqp_url.with_query(amqp_query)

        self.amqp_queue_name = amqp_queue_name if amqp_queue_name else self.name
        self.passive = passive

        params = pika.URLParameters(str(self.amqp_url))
        self.amqp_connection = pika.BlockingConnection(params)
        self.amqp_channel = self.amqp_connection.channel()

    async def qsize(self) -> int:
        message_count = 0
        try:
            amqp_method: "Method" = run_func(
                self.amqp_channel.queue_declare,
                queue=self.amqp_queue_name,
                passive=self.passive,
            )
            message_count: int = amqp_method.method.message_count
        except ChannelClosedByBroker as e:
            logger.error(e)

        return message_count

    async def empty(self) -> bool:
        count = await self.qsize()
        return True if count == 0 else False

    async def full(self) -> bool:
        if self.maxsize <= 0:
            return False

        count = await self.qsize()
        return True if count >= self.maxsize else False

    async def get_nowait(self) -> Any:
        message: MESSAGE_PACK = self.amqp_channel.basic_get(
            queue=self.amqp_queue_name, auto_ack=True
        )

        if message[2] is None:
            raise EmptyError("Queue is empty.")

        return pickle.loads(message[2])

    async def get(
        self, block: Optional[bool] = None, timeout: Optional[Number] = None
    ) -> Any:
        block = self.block if block is None else block
        timeout = self.timeout if timeout is None else timeout

        async def _wait_get() -> bytes:
            while True:
                message: MESSAGE_PACK = self.amqp_channel.basic_get(
                    queue=self.amqp_queue_name, auto_ack=True
                )
                if message[2] is None:
                    await asyncio.sleep(0.05)
                else:
                    return message[2]

        if block is True:
            value_bytes = await asyncio.wait_for(
                _wait_get(), timeout=(None if timeout <= 0 else timeout)
            )
            item = pickle.loads(value_bytes)
        else:
            item = await self.get_nowait()

        return item

    async def put_nowait(self, item: Any) -> None:
        pass

    async def put(
        self, item: Any, block: Optional[bool] = None, timeout: Optional[Number] = None
    ) -> None:
        pass

    async def join(self) -> None:
        while True:
            if self.empty() is True:
                break
            else:
                asyncio.sleep(0.05)

    async def task_done(self) -> None:
        pass

    async def close(self) -> None:
        await run_func(self.amqp_connection.close)
