import asyncio
import time
from numbers import Number
from pathlib import Path
from simple_pymq.broker.base import Broker
from typing import Any, Optional, Text

from simple_pymq.exceptions import FullError, EmptyError

try:
    import aiofiles

    is_aiofiles_installed = True
except ImportError:
    is_aiofiles_installed = False


class SimpleFileLock:
    def __init__(
        self,
        lock_path: Path,
        block: bool = True,
        timeout: Optional[Number] = None,
        poll_interval: float = 0.05,
        lock_timeout: float = 120.0,
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
        return self.lock_path.exists()

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
    pass
