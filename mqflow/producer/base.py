from abc import ABC
from numbers import Number
from typing import Optional, TYPE_CHECKING, Text, Type
import logging
import threading

from mqflow.config import settings

if TYPE_CHECKING:
    from mqflow.broker.base import Broker


logger = logging.getLogger(settings.logger_name)


class Producer(ABC):
    def __init__(
        self,
        broker: Type["Broker"],
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
