import asyncio
from typing import Any

from timing import time_me


@time_me
async def _print(what: Any) -> None:
    print(what)


@time_me
async def print_a() -> None:
    await asyncio.sleep(2)
    await _print('a')


@time_me
async def print_b() -> None:
    await asyncio.sleep(2)
    await _print('b')


async def main():
    await asyncio.gather(print_a(), print_b())


if __name__ == '__main__':
    asyncio.run(main())
