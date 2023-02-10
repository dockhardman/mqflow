import asyncio
from abc import ABC
from typing import List, Type, Union
from simple_pymq.broker.base import Broker
from simple_pymq.consumer.base import Consumer
from simple_pymq.producer.base import Producer


class MessageQueue(ABC):
    def __init__(self, *args, **kwargs):
        pass

    async def run(
        self,
        broker: Type[Broker],
        *args,
        producers: Union[List[Type[Producer]], Producer],
        consumers: Union[List[Type[Consumer]], Consumer],
        **kwargs,
    ):
        raise NotImplementedError


class SimpleMessageQueue(MessageQueue):
    def __init__(self, *args, **kwargs):
        pass

    async def run(
        self,
        broker: Type[Broker],
        *args,
        producers: Union[List[Type[Producer]], Producer],
        consumers: Union[List[Type[Consumer]], Consumer],
        **kwargs,
    ):
        producers: List[Type[Producer]] = (
            [producers] if isinstance(producers, Producer) else producers
        )
        consumers: List[Type[Consumer]] = (
            [consumers] if isinstance(consumers, Consumer) else consumers
        )

        if len(producers) == 0:
            raise ValueError(f"Should provide at least one producer.")
        if len(consumers) == 0:
            raise ValueError(f"Should provide at least one consumer.")

        tasks: List["asyncio.Task"] = []

        for producer in producers:
            _producer_task = asyncio.create_task(
                producer.produce(broker=broker, *args, **kwargs)
            )
            tasks.append(_producer_task)

        for consumer in consumers:
            _consumer_task = asyncio.create_task(
                consumer.listen(broker=broker, *args, **kwargs)
            )
            tasks.append(_consumer_task)

        await asyncio.gather(*tasks)
