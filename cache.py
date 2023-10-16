import asyncio
import functools
import inspect
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Literal, ParamSpec, TypeVar, cast

__MEMORY_CACHE = {"a": 1, "b": 2, "c": 3}  # initialize the cache
__CACHE_VERSION = "v1"
__CACHE_KEY_DELIMITER = "|"

ReturnT = TypeVar("ReturnT")
ParamT = ParamSpec("ParamT")


class CacheValue(Enum):
    NOT_FOUND = auto()


def cache_in_memory():
    """Cache data in memory"""

    def get_arg_default(function: Callable, arg_name: str):
        """Return defaults values of arguments of function
        :param function:
        :param arg_name
        :return: default value or None
        """
        args = get_function_parameters(function)
        for arg in args:
            if arg.name == arg_name:
                arg_def = arg.default
                return arg_def if arg_def != inspect.Parameter.empty else None
        return None

    def get_function_parameters(function: Callable) -> list:
        """Get function parameters
        :param function
        :return: Parameter list of function
        """
        return list(inspect.signature(function).parameters.values())

    def _get_function_identifier(function: Callable) -> str:
        """Get an unique function identifier"""

        return f"{function.__module__}.{function.__name__}"

    def make_cache_keys(function: Callable, args, kwargs) -> str:
        """Generate the cache keys
        :param function: the function to execute
        :param args: function args
        :param kwargs: function kwargs
        :return: key in str format
                 e.g function_name-arg_1-arg_2-arg_3
        """
        # we want to use the full qualifier name in case multiple modules contain same function name
        function_identifier = _get_function_identifier(function)
        keys = [__CACHE_VERSION, function_identifier]
        arg_parameters = get_function_parameters(function)
        if len(args) == 0:
            for parameter in arg_parameters:
                if parameter.name in kwargs:
                    keys.append(str(kwargs[parameter.name]))
                else:
                    keys.append(str(get_arg_default(function, parameter.name)))
            return __CACHE_KEY_DELIMITER.join(keys)
        else:
            keys += [str(value) for value in args]
            if len(args) == len(arg_parameters):
                return __CACHE_KEY_DELIMITER.join(keys)
            else:
                for i in range(len(args), len(arg_parameters)):
                    if arg_parameters[i].name in kwargs:
                        keys.append(str(kwargs[arg_parameters[i].name]))
                    else:
                        keys.append(
                            str(get_arg_default(function, arg_parameters[i].name))
                        )
                return __CACHE_KEY_DELIMITER.join(keys)

    def memory_cache_get(key: str) -> Any:
        """Get value in memory cache"""
        # we need this to distinguish cache-not-found and function_return=None
        return __MEMORY_CACHE.get(key) or CacheValue.NOT_FOUND

    def decorator(function: Callable[ParamT, ReturnT]) -> Callable[ParamT, ReturnT]:
        class CacheLogicWrapper:
            """
            The reason we implemented this as a context manager is to reduce code duplication
            instead of defining 2 identical functions (sync vs async)
            """

            def __init__(
                self,
                *args: ParamT.args,
                **kwargs: ParamT.kwargs,
            ) -> None:
                self.args = args
                self.kwargs = kwargs
                self.value = CacheValue.NOT_FOUND
                self.key = make_cache_keys(function, self.args, self.kwargs)
                self.new_value = False

            def __enter__(self) -> "CacheLogicWrapper":
                self.value = memory_cache_get(self.key)
                if self.value is CacheValue.NOT_FOUND:
                    self.value = function(*self.args, **self.kwargs)  # type: ignore
                return self

            def __exit__(
                self, exc_type: Any, exc_value: Any, traceback: Any
            ) -> Literal[False]:
                return False  # don't swallow exception

            def execute_sync(self) -> ReturnT:
                """Execute the cache logic sync"""

                return cast(ReturnT, self.value)

            async def execute_async(self) -> Any:
                """Execute the cache logic async"""
                if self.new_value:
                    self.value = await cast(Awaitable[Any], self.value)
                return self.value

        if inspect.iscoroutinefunction(function):
            # async version
            @functools.wraps(function)  # type: ignore
            async def async_wrapper(
                *args: ParamT.args, **kwargs: ParamT.kwargs  # type: ignore
            ) -> Any:
                with CacheLogicWrapper(*args, **kwargs) as cache_logic:
                    value = await cache_logic.execute_async()

                return value

            wrapper = async_wrapper

        else:
            # sync version
            @functools.wraps(function)
            def sync_wrapper(
                *args: ParamT.args, **kwargs: ParamT.kwargs  # type: ignore
            ) -> ReturnT:
                with CacheLogicWrapper(*args, **kwargs) as cache_logic:
                    value = cache_logic.execute_sync()

                return value

            wrapper = sync_wrapper

        return cast(Callable[ParamT, ReturnT], wrapper)

    return decorator


def _get(key: str) -> Any:
    return 1 if key == "a" else CacheValue.NOT_FOUND


@cache_in_memory()
def sync_get(key: str) -> Any:
    return _get(key)


@cache_in_memory()
async def async_get(key: str) -> Any:
    return _get(key)


async def run():
    print(sync_get("a"))
    print(sync_get("a"))
    print(await async_get("a"))
    print(await async_get("a"))


if __name__ == "__main__":
    asyncio.run(run())
