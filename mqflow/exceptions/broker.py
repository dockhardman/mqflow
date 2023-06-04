from asyncio import TimeoutError as AsyncioTimeoutError
from concurrent.futures import TimeoutError as ConcurrentTimeoutError
from multiprocessing import TimeoutError as MultiprocessingTimeoutError
from queue import Full, Empty


class EmptyError(Empty):
    pass


class FullError(Full):
    pass


class TimeoutError(
    AsyncioTimeoutError,
    ConcurrentTimeoutError,
    MultiprocessingTimeoutError,
    TimeoutError,
):
    pass
