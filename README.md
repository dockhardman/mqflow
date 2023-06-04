# mqflow #

[![dockhardman](https://circleci.com/gh/dockhardman/mqflow.svg?style=shield)](https://app.circleci.com/pipelines/github/dockhardman/mqflow)

mqflow is a simple Python message queue framework, providing an easy-to-use and efficient method to handle tasks asynchronously.

Github: https://github.com/dockhardman/mqflow

## Installation ##

```bash
pip install mqflow
```

## Features ##

- Easy-to-use: mqflow provides a Pythonic API that is both simple and effective for managing message queues.
- Flexibility: It supports different types of message queues such as FIFO, priority, and circular queues.
- Thread-Safe: mqflow uses Python's built-in queue library to ensure that your application is thread-safe.
- Customizable: mqflow allows you to customize your producer and consumer functions, providing great flexibility to fit your needs.

## Usage ##

Here is an example of a simple message queue pipeline in memory:

```python
from mqflow.broker import QueueBroker
from mqflow.producer import Producer
from mqflow.consumer import Consumer
from mqflow.pipeline import SequentialMessageQueue


def work(item, queue: "QueueBroker", *args, **kwargs):
    print(f"[{item}] -> [{queue}] -> [{''.join(args)}]")


task_num = 3
mq = SequentialMessageQueue(
    producers=[Producer(target=lambda: "Task sent", max_count=task_num)],
    consumers=[Consumer(target=work, args=("Task received",), max_count=task_num)],
    broker=QueueBroker(),
)
mq.run()
# [Task sent] -> [QueueBroker(name=QueueBroker, maxsize=0)] -> [Task received]
# [Task sent] -> [QueueBroker(name=QueueBroker, maxsize=0)] -> [Task received]
# [Task sent] -> [QueueBroker(name=QueueBroker, maxsize=0)] -> [Task received]
```

This creates a SequentialMessageQueue with a single producer that generates "Task sent" messages, and a single consumer that prints these messages with some additional information. The max_count parameter specifies the number of tasks the producer/consumer will handle before stopping. The broker manages the communication between producers and consumers.
