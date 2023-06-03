from abc import ABC
from math import modf
from numbers import Number
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    TYPE_CHECKING,
    Text,
    Tuple,
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


class ProducerBase(ABC):
    def __init__(
        self,
        broker: Type["BrokerBase"],
        *args,
        name: Text = "ProducerBase",
        block: bool = True,
        timeout: Optional[Number] = None,
        **kwargs,
    ):
        self.broker = broker
        self.name = name
        self.timeout = timeout
        self.block = block

        self._count: int = 0
        self._stop_event = threading.Event()

    @property
    def count(self) -> int:
        return self._count

    def count_add_one(self) -> None:
        self._count += 1

    def produce(self, **kwargs):
        raise NotImplementedError

    def stop(self) -> None:
        self._stop_event.set()

    def is_stop(self) -> bool:
        return self._stop_event.is_set()


class Producer(ProducerBase):
    def __init__(
        self,
        broker: Type["BrokerBase[T]"],
        *init_args,
        target: Callable[P, T],
        args: Tuple[Any, ...] = (),
        kwargs: Optional[Dict[Text, Any]] = None,
        name: Text = "Producer",
        block: bool = True,
        timeout: Optional[Number] = None,
        **init_kwargs,
    ):
        super().__init__(
            broker, *init_args, name=name, block=block, timeout=timeout, **init_kwargs
        )
        self.target = target
        self.target_args = args
        self.target_kwargs = kwargs or {}

    def produce(self, **kwargs):
        result = self.target(*self.target_args, **self.target_kwargs)
        self.broker.put(result, block=self.block, timeout=self.timeout)
        self.count_add_one()


class LoopingProducer(Producer):
    def __init__(
        self,
        broker: Type["BrokerBase[T]"],
        *init_args,
        target: Callable[P, T],
        args: Tuple[Any, ...] = (),
        kwargs: Optional[Dict[Text, Any]] = None,
        name: Text = "LoopingProducer",
        block: bool = True,
        timeout: Optional[Number] = None,
        loop_seconds: float = 0.0,
        max_count: Optional[int] = None,
        **init_kwargs,
    ):
        super().__init__(
            broker,
            *init_args,
            target=target,
            args=args,
            kwargs=kwargs,
            name=name,
            block=block,
            timeout=timeout,
            **init_kwargs,
        )
        self.loop_seconds = 0.0 if loop_seconds < 0 else loop_seconds
        if max_count is not None:
            self.max_count = int(max_count) if int(max_count) > 0 else None
        else:
            self.max_count = None

    def produce(self, **kwargs):
        count = 0
        while self.is_stop() is False and (
            self.max_count is None or count < self.max_count
        ):
            result = self.target(*self.target_args, **self.target_kwargs)
            self.broker.put(result, block=self.block, timeout=self.timeout)

            count += 1
            self.count_add_one()

            self._loop_check_stop(self.loop_seconds)

    def _loop_check_stop(self, seconds: float) -> None:
        _sleep_sec, _sleep_ns = modf(seconds)
        for _ in range(int(_sleep_sec)):
            if self.is_stop():
                return
            time.sleep(1)
        time.sleep(_sleep_ns)
