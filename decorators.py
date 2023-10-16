import functools
import inspect
from typing import Callable, ParamSpec, TypeVar, cast

ReturnT = TypeVar("ReturnT")
ParamT = ParamSpec("ParamT")


def decorator_a():
    def decorator(function: Callable[ParamT, ReturnT]) -> Callable[ParamT, ReturnT]:
        if inspect.iscoroutinefunction(function):
            # async version
            @functools.wraps(function)  # type: ignore
            async def async_wrapper(
                *args: ParamT.args, **kwargs: ParamT.kwargs  # type: ignore
            ) -> ReturnT:
                print("a")
                return await function(*args, **kwargs)

            wrapper = async_wrapper

        else:
            # sync version
            @functools.wraps(function)
            def sync_wrapper(
                *args: ParamT.args, **kwargs: ParamT.kwargs  # type: ignore
            ) -> ReturnT:
                print("a")
                return function(*args, **kwargs)

            wrapper = sync_wrapper

        return cast(Callable[ParamT, ReturnT], wrapper)

    return decorator


def decorator_b():
    def decorator(function: Callable[ParamT, ReturnT]) -> Callable[ParamT, ReturnT]:
        if inspect.iscoroutinefunction(function):
            # async version
            @functools.wraps(function)  # type: ignore
            async def async_wrapper(
                *args: ParamT.args, **kwargs: ParamT.kwargs  # type: ignore
            ) -> ReturnT:
                print("b")
                return await function(*args, **kwargs)

            wrapper = async_wrapper

        else:
            # sync version
            @functools.wraps(function)
            def sync_wrapper(
                *args: ParamT.args, **kwargs: ParamT.kwargs  # type: ignore
            ) -> ReturnT:
                print("b")
                return function(*args, **kwargs)

            wrapper = sync_wrapper

        return cast(Callable[ParamT, ReturnT], wrapper)

    return decorator


@decorator_a()
@decorator_b()
def foo() -> None:
    print("foo")


@decorator_b()
@decorator_a()
def bar() -> None:
    print("bar")


if __name__ == "__main__":
    foo()
    bar()
