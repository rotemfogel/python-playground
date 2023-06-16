import asyncio
import functools
import time
from typing import Callable, ParamSpec, TypeVar

import loguru

ParamT = ParamSpec("ParamT")
ReturnT = TypeVar("ReturnT")

_LOG = loguru.logger


def time_me():
    def decorator(fn: Callable[ParamT, ReturnT]) -> Callable[ParamT, ReturnT]:
        @functools.wraps(fn)
        def wrapper(
            *args: ParamT.args,
            **kwargs: ParamT.kwargs,
        ) -> ReturnT:
            fn_name = getattr(fn, "__name__", "Unknown")
            start_t = time.time()
            if args and kwargs:
                result = fn(args, kwargs)
            else:
                result = fn()
            _LOG.info("%s took %.2fs" % (fn_name, time.time() - start_t))
            return result

        return wrapper

    return decorator


@time_me()
def time_sleep():
    asyncio.run(asyncio.sleep(1))
    _LOG.info("done")


if __name__ == "__main__":
    time_sleep()
