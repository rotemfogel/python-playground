import asyncio
import time
from typing import Callable, Any


def time_me(func: Callable[[Any], Any]):
    async def decorator(*args: Any, **params: Any):
        async def process():
            if asyncio.iscoroutinefunction(func):
                # function is a coroutine
                return await func(*args, **params)
            else:
                # regular function
                return func(*args, **params)

        start = time.perf_counter()
        result = await process()
        print("%s took %2.4f sec" % (func.__name__, time.perf_counter() - start))
        return result

    return decorator
