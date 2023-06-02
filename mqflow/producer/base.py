from abc import ABC
from numbers import Number
from typing import Any, Optional, Text, Type
import logging
import threading
import time

from mqflow.broker.base import Broker
from mqflow.config import settings


logger = logging.getLogger(settings.logger_name)


class Producer(ABC):
    def __init__(
        self,
        broker: Type[Broker],
        *args,
        name: Text = "Producer",
        block: bool = True,
        timeout: Optional[Number] = None,
        **kwargs,
    ):
        self.broker = broker
        self.name = name
        self.timeout = timeout
        self.block = block

        self._stop_event = threading.Event()

    def produce(self, **kwargs):
        raise NotImplementedError

    def stop(self) -> None:
        self._stop_event.set()

    def is_stop(self) -> bool:
        self._stop_event.is_set()


class CountDownProducer(Producer):
    def __init__(
        self,
        broker: Type[Broker],
        *args,
        name: Text = "CountDownProducer",
        block: bool = True,
        timeout: Optional[Number] = None,
        interval_seconds: float = 1.0,
        max_count: Optional[int] = None,
        put_value: Any = 1,
        **kwargs,
    ):
        super().__init__(
            broker, *args, name=name, block=block, timeout=timeout, **kwargs
        )
        self.interval_seconds = interval_seconds if interval_seconds > 0 else 1.0
        if max_count is not None:
            self.max_count = int(max_count) if int(max_count) > 0 else None
        else:
            self.max_count = None
        self.put_value = put_value

    def produce(self, **kwargs):
        count = 0
        while self.is_stop() is False and (
            count < self.max_count or self.max_count is None
        ):
            self.broker.put(self.put_value, block=self.block, timeout=self.timeout)

            count += 1

            _sleep_sec, _sleep_ns = divmod(self.interval_seconds, 1)
            for _ in range(int(_sleep_sec)):
                if self.is_stop():
                    break
                time.sleep(1)
            time.sleep(_sleep_ns)
