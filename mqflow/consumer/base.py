from abc import ABC
from typing import Any, Callable, Dict, Generic, Optional, Text, Tuple, Type, TypeVar
from typing_extensions import ParamSpec
import logging
import threading
import time

from mqflow.broker.base import BrokerBase
from mqflow.config import settings
from mqflow.exceptions import EmptyError


logger = logging.getLogger(settings.logger_name)

P = ParamSpec("P")
S = TypeVar("S")
T = TypeVar("T")


class ConsumerBase(ABC, Generic[P, S, T]):
    def __init__(
        self,
        *init_args,
        name: Text = "ConsumerBase",
        block: bool = True,
        timeout: Optional[float] = None,
        max_count: Optional[int] = None,
        **init_kwargs,
    ):
        self.name = name
        if max_count is not None:
            self.max_count = int(max_count) if int(max_count) > 0 else None
        else:
            self.max_count = None
        self.block = block
        self.timeout = timeout

        self._count = 0
        self._stop_event = threading.Event()

    def listen(
        self,
        broker: Type[BrokerBase[T]],
        *args,
        block: Optional[bool] = None,
        timeout: Optional[float] = None,
        max_count: Optional[int] = None,
        **kwargs,
    ):
        block = self.block if block is None else block
        timeout = self.timeout if timeout is None else timeout
        max_count = self.max_count if max_count is None else max_count

        count = 0
        time_start = time.time()
        while self.is_stop() is False and (
            self.max_count is None or count < self.max_count
        ):
            try:
                item = broker.get(block=block, timeout=1.0)
            except EmptyError as e:
                if timeout is not None and time.time() - time_start > timeout:
                    self.stop()
                    raise e
                continue
            except KeyboardInterrupt:
                self.stop()
                return
            except Exception as e:
                logger.exception(e)
                self.stop()
                return

            self.consume(item, broker)
            broker.task_done()

            count += 1
            self.count_add_one()

    def consume(self, item: T, broker: Type[BrokerBase[T]], *args, **kwargs) -> None:
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


class Consumer(ConsumerBase[P, S, T]):
    def __init__(
        self,
        target: Callable[P, S],
        args: Tuple[Any, ...] = (),
        kwargs: Optional[Dict[Text, Any]] = None,
        *init_args,
        name: Text = "Consumer",
        block: bool = True,
        timeout: Optional[float] = None,
        max_count: Optional[int] = None,
        **init_kwargs,
    ):
        super().__init__(
            name=name,
            *init_args,
            block=block,
            timeout=timeout,
            max_count=max_count,
            **init_kwargs,
        )

        self.target = target
        self.args = args
        self.kwargs = kwargs or {}

    def consume(self, item: T, broker: Type[BrokerBase[T]], *args, **kwargs) -> None:
        self.target(item, broker, *self.args, **self.kwargs)
