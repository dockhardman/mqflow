import asyncio
import pickle
from numbers import Number
from typing import Any, List, Optional, Text, Tuple

from simple_pymq.broker.base import Broker
from simple_pymq.config import logger
from simple_pymq.exceptions import FullError, EmptyError


is_redis_installed = True
try:
    from redis.asyncio import Redis
except ImportError:
    is_redis_installed = False


class RedisBroker(Broker):
    def __init__(
        self,
        host: Text,
        port: int,
        db: int,
        username: Optional[Text] = None,
        password: Optional[Text] = None,
        name: Text = "RedisBroker",
        maxsize: int = 0,
        *args,
        key_base_name: Optional[Text] = None,
        key_prefix: Text = "",
        key_postfix: Text = "",
        key_expire: int = 60 * 60 * 24 * 7,
        block: bool = True,
        timeout: Optional[Number] = None,
        **kwargs,
    ):
        if is_redis_installed is False:
            raise ImportError("Package 'aiofiles' is not installed.")

        super(RedisBroker, self).__init__(
            name=name, maxsize=maxsize, *args, block=block, timeout=timeout, **kwargs
        )

        self.host = host
        self.port = port
        self.db = db

        self.key_name = (
            key_prefix + (key_base_name if key_base_name else self.name) + key_postfix
        )
        self.key_expire = key_expire

        self.redis_client = Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            username=username,
            password=password,
        )
        logger.debug(
            f"{self.name}: {username}:******@{self.host}:{self.port}.{self.db}"
        )

    async def qsize(self) -> int:
        return await self.redis_client.llen(self.key_name)

    async def empty(self) -> bool:
        count = await self.qsize()
        return True if count == 0 else False

    async def full(self) -> bool:
        if self.maxsize <= 0:
            return False

        count = await self.qsize()
        return True if count >= self.maxsize else False

    async def get_nowait(self) -> Any:
        data: List[bytes] = await self.redis_client.rpop(self.key_name, count=1)

        if data is None or len(data) == 0:
            raise EmptyError("Queue is empty.")

        assert (len(data) == 1, "Only one item should be returned.")
        value_bytes = data[0]

        return pickle.loads(value_bytes)

    async def get(
        self, block: Optional[bool] = None, timeout: Optional[Number] = None
    ) -> Any:
        block = self.block if block is None else block
        timeout = self.timeout if timeout is None else timeout

        if block is True:
            data: Optional[Tuple[bytes, bytes]] = await self.redis_client.brpop(
                self.key_name, timeout=timeout
            )

            if data is None or len(data) == 0:
                raise asyncio.TimeoutError("Queue is empty.")

            _, value_bytes = data
            item = pickle.loads(value_bytes)
        else:
            item = await self.get_nowait()

        return item

    async def put_nowait(self, item: Any) -> None:
        if await self.full() is True:
            raise FullError("Queue is full.")
        pipe = self.redis_client.pipeline()
        pipe.lpush(self.key_name, pickle.dumps(item))
        pipe.expire(self.key_name, time=self.key_expire)
        await pipe.execute()

    async def put(
        self, item: Any, block: Optional[bool] = None, timeout: Optional[Number] = None
    ) -> None:
        block = self.block if block is None else block
        timeout = self.timeout if timeout is None else timeout

        async def _wait_put(item: Any):
            while True:
                if await self.full() is True:
                    await asyncio.sleep(0.05)
                else:
                    pipe = self.redis_client.pipeline()
                    pipe.lpush(self.key_name, pickle.dumps(item))
                    pipe.expire(self.key_name, time=self.key_expire)
                    await pipe.execute()
                    break

        if block is True:
            try:
                await asyncio.wait_for(_wait_put(item=item), timeout=timeout)
            except asyncio.TimeoutError:
                raise asyncio.TimeoutError("Queue is full.")

        else:
            item = await self.put_nowait(item=item)

    async def join(self) -> None:
        while True:
            if self.empty() is True:
                break
            else:
                asyncio.sleep(0.05)

    async def task_done(self) -> None:
        self.unfinished -= 1
