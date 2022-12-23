import asyncio

from timing import time_me


@time_me
async def print_a():
    await asyncio.sleep(2)
    print('a')


@time_me
async def print_b():
    await asyncio.sleep(2)
    print('b')


async def main():
    await asyncio.gather(print_a(), print_b())


if __name__ == '__main__':
    asyncio.run(main())
