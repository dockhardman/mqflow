import asyncio
import pickle
import time
from numbers import Number
from pathlib import Path
from simple_pymq.broker.base import Broker
from typing import Any, AsyncGenerator, List, Optional, Text, Type

from simple_pymq.exceptions import FullError, EmptyError


is_aiofiles_installed = True
try:
    import aiofiles
except ImportError:
    is_aiofiles_installed = False


class SimpleFileLock:
    def __init__(
        self,
        lock_path: Path,
        block: bool = True,
        timeout: Optional[Number] = None,
        poll_interval: float = 0.05,
        lock_timeout: float = 5.0,
    ):
        self.lock_path = Path(lock_path).resolve()
        self.block = block
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.lock_timeout = lock_timeout

    async def __aenter__(self) -> None:
        await self.acquire()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()

    async def acquire(
        self,
        block: Optional[bool] = None,
        timeout: Optional[Number] = None,
        poll_interval: Optional[float] = None,
    ) -> bool:
        block = self.block if block is None else block
        timeout = self.timeout if timeout is None else timeout
        poll_interval = self.poll_interval if poll_interval is None else poll_interval

        if self.lock_path.parent.exists() is False:
            self.lock_path.parent.mkdir(parents=True)

        lock_acquired = await asyncio.wait_for(
            self._touch_lock(block=block, poll_interval=poll_interval), timeout=timeout
        )
        return lock_acquired

    async def release(self):
        self.lock_path.unlink(missing_ok=True)

    async def locked(self):
        is_locked = True

        if self.lock_path.exists() is True:
            if time.time() - self.lock_path.stat().st_mtime >= self.lock_timeout:
                is_locked = False
            else:
                is_locked = True
        else:
            is_locked = False

        return is_locked

    async def _touch_lock(self, block: bool, poll_interval: float) -> bool:
        while True:
            try:
                self.lock_path.touch(exist_ok=False)
                return True

            except FileExistsError:
                if time.time() - self.lock_path.stat().st_mtime >= self.lock_timeout:
                    self.lock_path.touch(exist_ok=True)
                    return True
                elif block is False:
                    return False

                await asyncio.sleep(poll_interval)


class SimpleFileBroker(Broker):
    def __init__(
        self,
        file: Text,
        name: Text = "SimpleFileBroker",
        maxsize: int = 0,
        *args,
        block: bool = True,
        timeout: Optional[Number] = None,
        lock_timeout: float = 5.0,
        **kwargs
    ):
        if is_aiofiles_installed is False:
            raise ImportError("Package 'aiofiles' is not installed.")

        super(SimpleFileBroker, self).__init__(
            name=name, maxsize=maxsize, *args, block=block, timeout=timeout, **kwargs
        )

        self.file = Path(file).resolve().absolute()
        self.lock_path = self.file.with_name(self.file.name + ".lock")
        self.lock_timeout = lock_timeout
        self.unfinished = 0

        if self.file.parent.exists() is False:
            self.file.parent.mkdir(parents=True)
        if self.file.exists() is False:
            with open(self.file, "wb") as f:
                f.write(pickle.dumps([]))

    async def empty(self) -> bool:
        count = await self._lock_count()
        return True if count == 0 else False

    async def full(self) -> bool:
        count = await self._lock_count()
        return True if count >= self.maxsize else False

    async def get(
        self, block: Optional[bool] = None, timeout: Optional[Number] = None
    ) -> Any:
        block = self.block if block is None else block
        timeout = self.timeout if timeout is None else timeout

        if block is True:
            item = await asyncio.wait_for(self._lock_wait_and_get(), timeout=timeout)
        else:
            item = await self.get_nowait()

        return item

    async def get_nowait(self) -> Any:
        return await self._lock_get_nowait()

    async def join(self) -> None:
        while True:
            if self.empty() is True:
                break
            else:
                asyncio.sleep(0.05)

    async def put(
        self, item: Any, block: Optional[bool] = None, timeout: Optional[Number] = None
    ) -> None:
        block = self.block if block is None else block
        timeout = self.timeout if timeout is None else timeout

        if block is True:
            item = await asyncio.wait_for(
                self._lock_wait_and_put(item=item), timeout=timeout
            )
        else:
            item = await self.put_nowait(item=item)

    async def put_nowait(self, item: Any) -> None:
        await self._lock_put_nowait(item=item)

    async def qsize(self) -> int:
        return await self._lock_count()

    async def task_done(self) -> None:
        self.unfinished -= 1

    async def _lock_wait_and_get(self) -> Any:
        while True:
            async with SimpleFileLock(
                self.lock_path, block=True, lock_timeout=self.lock_timeout
            ):
                items = await self._read_pickle()

                if len(items) > 0:
                    item = items.pop(0)
                    await self._dump_pickle(items=items)
                    return item

            await asyncio.sleep(0.05)

    async def _lock_get_nowait(self) -> Any:
        async with SimpleFileLock(
            self.lock_path, block=False, lock_timeout=self.lock_timeout
        ):
            items = await self._read_pickle()
            if len(items) > 0:
                item = items.pop(0)
                await self._dump_pickle(items=items)
                return item
            else:
                raise EmptyError

    async def _lock_wait_and_put(self, item: Any) -> None:
        while True:
            async with SimpleFileLock(
                self.lock_path, block=True, lock_timeout=self.lock_timeout
            ):
                items = await self._read_pickle()
                if len(items) < self.maxsize:
                    items.append(item)
                    await self._dump_pickle(items=items)
                    return None

            await asyncio.sleep(0.05)

    async def _lock_put_nowait(self, item: Any) -> None:
        async with SimpleFileLock(
            self.lock_path, block=False, lock_timeout=self.lock_timeout
        ):
            items = await self._read_pickle()
            if len(items) < self.maxsize:
                items.append(item)
                await self._dump_pickle(items=items)
                return None
            else:
                raise FullError

    async def _lock_count(self) -> int:
        async with SimpleFileLock(
            self.lock_path, block=True, lock_timeout=self.lock_timeout
        ):
            items = await self._read_pickle()
            return len(items)

    async def _read_pickle(self) -> List[Any]:
        async with aiofiles.open(self.file, "rb") as f:
            data = await f.read()
            return pickle.loads(data)

    async def _dump_pickle(self, items: List[Any]) -> None:
        async with aiofiles.open(self.file, "wb") as f:
            await f.write(pickle.dumps(items))
