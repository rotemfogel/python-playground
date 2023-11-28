import asyncio
from typing import TypedDict


class Person(TypedDict):
    id: str
    name: str


class TV(TypedDict):
    width: float
    height: float


async def get_str_list() -> list[str]:
    return ["a", "b", "c"]


async def get_str() -> str:
    return "a"


async def get_int_list() -> list[int]:
    return [1, 2, 3]


async def get_int() -> int:
    return 0


async def get_person_list() -> list[Person]:
    return [Person(id=1, name="A"), Person(id=2, name="B")]


async def get_person() -> Person:
    return Person(id=1, name="A")


async def get_tv() -> TV:
    return TV(height=25.1, width=44.1)


async def run() -> None:
    get_int_list_task = get_int_list()
    get_int_task = get_int()
    get_str_list_task = get_str_list()
    get_str_task = get_str()
    get_person_task = get_person()
    get_person_list_task = get_person_list()
    get_tv_task = get_tv()

    (li, i, ls, s, lp, p, t) = await asyncio.gather(
        get_int_list_task,
        get_int_task,
        get_str_list_task,
        get_str_task,
        get_person_list_task,
        get_person_task,
        get_tv_task,
    )
    print(f"{li} ({type(li)})")
    print(f"{i} ({type(i)})")
    print(f"{ls} ({type(ls)})")
    print(f"{s} ({type(s)})")
    print(f"{lp} ({type(lp)})")
    print(f"{p} ({type(p)})")
    print(f"{t} ({type(t)})")


if __name__ == "__main__":
    asyncio.run(run())
