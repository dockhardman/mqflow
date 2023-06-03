from asyncio import TimeoutError as AsyncioTimeoutError
from concurrent.futures import TimeoutError as ConcurrentTimeoutError
from multiprocessing import TimeoutError as MultiprocessingTimeoutError


class EmptyError(Exception):
    pass


class FullError(Exception):
    pass


class TimeoutError(
    AsyncioTimeoutError,
    ConcurrentTimeoutError,
    MultiprocessingTimeoutError,
    TimeoutError,
):
    pass
