from threading import Thread
from typing import List, Optional, TYPE_CHECKING, Text, Type, TypeVar
from typing_extensions import ParamSpec
import logging

from mqflow.pipeline.base import MessageQueueBase
from mqflow.config import settings

if TYPE_CHECKING:
    from mqflow.broker.base import BrokerBase
    from mqflow.consumer.base import ConsumerBase
    from mqflow.producer.base import ProducerBase


logger = logging.Logger(settings.logger_name)

P = ParamSpec("P")
S = TypeVar("S")
T = TypeVar("T")


class SequentialMessageQueue(MessageQueueBase[P, S, T]):
    def __init__(
        self,
        *args,
        name: Text = "SequentialMessageQueue",
        producers: Optional[List[Type["ProducerBase[T]"]]] = None,
        consumers: Optional[List[Type["ConsumerBase[P, S, T]"]]] = None,
        broker: Optional[Type["BrokerBase[T]"]] = None,
        **kwargs,
    ):
        super().__init__(
            *args, producers=producers, consumers=consumers, broker=broker, **kwargs
        )

    def run(self, *args, **kwargs):
        if not self.producers or not self.consumers or self.broker is None:
            raise ValueError("No producers, consumers, or broker defined")

        producer_threads = [
            Thread(target=producer.publish, kwargs=dict(broker=self.broker))
            for producer in self.producers
        ]
        consumer_threads = [
            Thread(target=consumer.listen, kwargs=dict(broker=self.broker))
            for consumer in self.consumers
        ]

        for thread in producer_threads:
            thread.start()
        for thread in consumer_threads:
            thread.start()

        try:
            for thread in producer_threads:
                thread.join()
            for thread in consumer_threads:
                thread.join()

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt")
            [producer.stop() for producer in self.producers]
            [consumer.stop() for consumer in self.consumers]
            for thread in producer_threads:
                thread.join()
            for thread in consumer_threads:
                thread.join()

        except Exception as e:
            logger.exception(e)
            logger.info(f"Raise exception stop: {e}")
            [producer.stop() for producer in self.producers]
            [consumer.stop() for consumer in self.consumers]
            for thread in producer_threads:
                thread.join()
            for thread in consumer_threads:
                thread.join()

        finally:
            self.finish()
