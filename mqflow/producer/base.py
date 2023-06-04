from abc import ABC
from math import modf
from numbers import Number
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    Text,
    Tuple,
    TYPE_CHECKING,
    Type,
    TypeVar,
)
from typing_extensions import ParamSpec
import logging
import threading
import time

from mqflow.config import settings

if TYPE_CHECKING:
    from mqflow.broker.base import BrokerBase


logger = logging.getLogger(settings.logger_name)


T = TypeVar("T")
P = ParamSpec("P")


class ProducerBase(ABC, Generic[T]):
    def __init__(
        self,
        *init_args,
        name: Text = "ProducerBase",
        block: bool = True,
        timeout: Optional[Number] = None,
        max_count: Optional[int] = None,
        timer_seconds: Number = 0.0,
        interval_seconds: Number = 0.0,
        **init_kwargs,
    ):
        self.name = name
        self.block = block
        self.timeout = timeout
        if max_count is not None:
            self.max_count = int(max_count) if int(max_count) > 0 else None
        else:
            self.max_count = None
        self.timer_seconds = timer_seconds
        self.interval_seconds = interval_seconds

        self._count: int = 0
        self._stop_event = threading.Event()

    def publish(
        self,
        broker: Type["BrokerBase[T]"],
        block: Optional[bool] = None,
        timeout: Optional[Number] = None,
        **kwargs,
    ):
        block = self.block if block is None else block
        timeout = self.timeout if timeout is None else timeout

        self._sleep_with_stop_event(self.timer_seconds)

        count = 0
        while self.is_stop() is False and (
            self.max_count is None or count < self.max_count
        ):
            result = self.produce(**kwargs)
            broker.put(result, block=self.block, timeout=self.timeout)

            count += 1
            self.count_add_one()

            self._sleep_with_stop_event(self.interval_seconds)

    def produce(self, **kwargs) -> T:
        raise NotImplementedError

    @property
    def count(self) -> int:
        return self._count

    def count_add_one(self) -> None:
        self._count += 1

    def stop(self) -> None:
        self._stop_event.set()

    def is_stop(self) -> bool:
        return self._stop_event.is_set()

    def _sleep_with_stop_event(self, seconds: Number) -> None:
        _sleep_sec, _sleep_ns = modf(seconds)
        for _ in range(int(_sleep_sec)):
            if self.is_stop():
                return
            time.sleep(1)
        time.sleep(_sleep_ns)


class Producer(ProducerBase[T]):
    def __init__(
        self,
        target: Callable[P, T],
        args: Tuple[Any, ...] = (),
        kwargs: Optional[Dict[Text, Any]] = None,
        *init_args,
        name: Text = "ProducerBase",
        block: bool = True,
        timeout: Optional[Number] = None,
        max_count: Optional[int] = None,
        timer_seconds: Number = 0.0,
        interval_seconds: Number = 0.0,
        **init_kwargs,
    ):
        super().__init__(
            *init_args,
            name=name,
            block=block,
            timeout=timeout,
            max_count=max_count,
            timer_seconds=timer_seconds,
            interval_seconds=interval_seconds,
            **init_kwargs,
        )
        self.target = target
        self.target_args = args
        self.target_kwargs = kwargs or {}

    def produce(self, **kwargs) -> T:
        result = self.target(*self.target_args, **self.target_kwargs)
        return result
