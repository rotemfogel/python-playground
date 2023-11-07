import asyncio


def foo() -> str:
    return "foo"


async def bar() -> str:
    return "bar"


async def process(f):
    result = await asyncio.to_thread(f)
    print(type(result))
    if asyncio.iscoroutine(result):
        print(await result)
    else:
        print(result)


if __name__ == "__main__":
    asyncio.run(process(foo))
    asyncio.run(process(bar))
