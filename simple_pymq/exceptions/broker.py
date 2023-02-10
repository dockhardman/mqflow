import asyncio


class EmptyError(Exception):
    pass


class FullError(Exception):
    pass


class TimeoutError(asyncio.TimeoutError):
    pass
