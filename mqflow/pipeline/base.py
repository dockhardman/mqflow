from abc import ABC
from typing import Generic, List, Optional, Type, TypeVar
from typing_extensions import ParamSpec
import threading

from mqflow.broker.base import BrokerBase
from mqflow.consumer.base import ConsumerBase
from mqflow.producer.base import ProducerBase


P = ParamSpec("P")
S = TypeVar("S")
T = TypeVar("T")


class MessageQueueBase(ABC, Generic[P, S, T]):
    def __init__(
        self,
        *args,
        producers: Optional[List[Type["ProducerBase[T]"]]] = None,
        consumers: Optional[List[Type["ConsumerBase[P, S, T]"]]] = None,
        broker: Optional[Type["BrokerBase[T]"]] = None,
        **kwargs,
    ):
        self.producers = producers or []
        self.consumers = consumers or []
        self.broker = broker

        self._stop_event = threading.Event()

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def finish(self, *args, **kwargs):
        self.broker.close()

    def stop(self) -> None:
        self._stop_event.set()
        for consumer in self.consumers:
            consumer.stop()
        for producer in self.producers:
            producer.stop()

    def is_stop(self) -> bool:
        return self._stop_event.is_set()
