from math import modf
from numbers import Number
from typing import Any, Optional, TYPE_CHECKING, Text, Type
import logging
import time

from mqflow.config import settings
from mqflow.producer.base import ProducerBase

if TYPE_CHECKING:
    from mqflow.broker.base import Broker


logger = logging.getLogger(settings.logger_name)


class TimerProducer(ProducerBase):
    def __init__(
        self,
        broker: Type["Broker"],
        *args,
        name: Text = "TimerProducer",
        block: bool = True,
        timeout: Optional[Number] = None,
        timer_seconds: float = 0.0,
        put_value: Any = 1,
        **kwargs,
    ):
        super().__init__(
            broker, *args, name=name, block=block, timeout=timeout, **kwargs
        )
        self.timer_seconds = 0.0 if timer_seconds < 0 else timer_seconds
        self.put_value = put_value

    def produce(self, **kwargs):
        self._loop_check_stop(self.timer_seconds)

        if self.is_stop():
            return

        self.broker.put(self.put_value, block=self.block, timeout=self.timeout)
        self.count_add_one()

    def _loop_check_stop(self, seconds: float) -> None:
        _sleep_sec, _sleep_ns = modf(self.timer_seconds)
        for _ in range(int(_sleep_sec)):
            if self.is_stop():
                return
            time.sleep(1)
        time.sleep(_sleep_ns)


class LoopingTimerProducer(TimerProducer):
    def __init__(
        self,
        broker: Type["Broker"],
        *args,
        name: Text = "LoopingTimerProducer",
        block: bool = True,
        timeout: Optional[Number] = None,
        timer_seconds: float = 0.0,
        loop_seconds: float = 0.0,
        max_count: Optional[int] = None,
        put_value: Any = 1,
        **kwargs,
    ):
        super().__init__(
            broker,
            *args,
            name=name,
            block=block,
            timeout=timeout,
            timer_seconds=timer_seconds,
            **kwargs,
        )
        self.loop_seconds = 0.0 if loop_seconds < 0 else loop_seconds
        if max_count is not None:
            self.max_count = int(max_count) if int(max_count) > 0 else None
        else:
            self.max_count = None
        self.put_value = put_value

    def produce(self, **kwargs):
        self._loop_check_stop(self.timer_seconds)

        if self.is_stop():
            return

        count = 0
        while self.is_stop() is False and (
            self.max_count is None or count < self.max_count
        ):
            self.broker.put(self.put_value, block=self.block, timeout=self.timeout)

            count += 1
            self.count_add_one()

            self._loop_check_stop(self.loop_seconds)
