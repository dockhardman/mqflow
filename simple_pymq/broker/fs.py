import asyncio
import time
from numbers import Number
from pathlib import Path
from simple_pymq.broker.base import Broker
from typing import Any, AsyncGenerator, Optional, Text, Type

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
        self.file.touch(exist_ok=True)

    async def empty(self) -> bool:
        async with SimpleFileLock(self.lock_path, lock_timeout=self.lock_timeout):
            async for _ in self._read_lines(filepath=self.file):
                return False
            else:
                return True

    async def full(self) -> bool:
        if self.maxsize <= 0:
            return False

        count = 0
        async with SimpleFileLock(self.lock_path, lock_timeout=self.lock_timeout):
            async for _ in self._read_lines(filepath=self.file):
                count += 1
                if count >= self.maxsize:
                    return True
        return False

    async def get(self, block: bool = True, timeout: Optional[Number] = None) -> bytes:
        if block is True:
            item = await asyncio.wait_for(self._wait_and_get(), timeout=timeout)
        else:
            item = await self.get_nowait()

        return item

    async def get_nowait(self) -> bytes:
        async with SimpleFileLock(
            self.lock_path, block=False, lock_timeout=self.lock_timeout
        ):
            item = self._get_first_and_write_others()

        return item

    async def join(self) -> None:
        while True:
            if self.empty() is True:
                break
            else:
                asyncio.sleep(0.05)

    async def put(
        self, item: Any, block: bool = True, timeout: Optional[Number] = None
    ) -> None:
        if block is True:
            item = await asyncio.wait_for(self._wait_and_put(), timeout=timeout)
        else:
            item = await self.put_nowait(item=item)

    async def put_nowait(self, item: Any) -> None:
        async with SimpleFileLock(
            self.lock_path, block=False, lock_timeout=self.lock_timeout
        ):
            item = self._put_at_the_end(item_bytes=item)

    async def qsize(self) -> int:
        count = 0
        async with SimpleFileLock(
            self.lock_path, block=False, lock_timeout=self.lock_timeout
        ):
            async for _ in self._read_lines(filepath=self.file):
                count += 1
        return count

    async def task_done(self) -> None:
        self.unfinished -= 1

    async def _wait_and_get(self) -> Any:

        while True:
            async with SimpleFileLock(
                self.lock_path, block=False, lock_timeout=self.lock_timeout
            ):
                empty = True
                async for _ in self._read_lines(filepath=self.file):
                    empty = False
                    break

                if empty is False:
                    item = await self._get_first_and_write_others()
                    return item

            await asyncio.sleep(0.05)

    async def _wait_and_put(self, item_bytes: bytes) -> None:

        while True:
            async with SimpleFileLock(
                self.lock_path, block=False, lock_timeout=self.lock_timeout
            ):
                full = False
                count = 0
                async for _ in self._read_lines(filepath=self.file):
                    count += 0
                    if count >= self.maxsize:
                        full = True
                        break

                if full is False:
                    await self._put_at_the_end(item_bytes=item_bytes)
                    return None

            await asyncio.sleep(0.05)

    async def _get_first_and_write_others(self) -> Any:
        head = None
        tails = []

        async for line in self._read_lines(filepath=self.file):
            if head is None:
                head = line
            else:
                tails.append(line)

        if head is None:
            raise EmptyError

        async with aiofiles.open(self.file, "w") as f:
            f.writelines(tails)

        return head

    async def _put_at_the_end(self, item_bytes: bytes) -> None:
        async with aiofiles.open(self.file, "ab") as f:
            f.write(b"\n" + item_bytes)

    async def _read_lines(self, filepath: Path) -> AsyncGenerator[bytes, None]:
        async with aiofiles.open(filepath, "rb") as f:
            async for line in f:
                yield line
