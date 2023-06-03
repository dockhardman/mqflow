from numbers import Number
from typing import Any, Optional, TYPE_CHECKING, Text, Type
import logging
import time

from mqflow.config import settings
from mqflow.producer.base import Producer

if TYPE_CHECKING:
    from mqflow.broker.base import Broker


logger = logging.getLogger(settings.logger_name)


class CountDownProducer(Producer):
    def __init__(
        self,
        broker: Type["Broker"],
        *args,
        name: Text = "CountDownProducer",
        block: bool = True,
        timeout: Optional[Number] = None,
        interval_seconds: float = 0.0,
        max_count: Optional[int] = None,
        put_value: Any = 1,
        **kwargs,
    ):
        super().__init__(
            broker, *args, name=name, block=block, timeout=timeout, **kwargs
        )
        self.interval_seconds = 0.0 if interval_seconds < 0 else interval_seconds
        if max_count is not None:
            self.max_count = int(max_count) if int(max_count) > 0 else None
        else:
            self.max_count = None
        self.put_value = put_value

    def produce(self, **kwargs):
        count = 0
        while self.is_stop() is False and (
            self.max_count is None or count < self.max_count
        ):
            self.broker.put(self.put_value, block=self.block, timeout=self.timeout)

            count += 1
            self.count_add_one()

            _sleep_sec, _sleep_ns = divmod(self.interval_seconds, 1)
            for _ in range(int(_sleep_sec)):
                if self.is_stop():
                    break
                time.sleep(1)
            time.sleep(_sleep_ns)
