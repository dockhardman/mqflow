import asyncio
from simple_pymq import (
    PrintConsumer,
    QueueBroker,
    SimpleMessageQueue,
    TimeCounterProducer,
)


async def main():
    q = QueueBroker(maxsize=32)
    p = TimeCounterProducer(
        count_seconds=1.0, max_produce_count=3, put_value="Message here."
    )
    c = PrintConsumer(max_consume_count=3)
    mq = SimpleMessageQueue()

    await mq.run(broker=q, producers=p, consumers=c)
    print("All tasks done!")


asyncio.run(main())
