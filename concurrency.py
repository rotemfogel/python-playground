from typing import List, Dict
import asyncio
import time


async def get_posts() -> List[Dict[any, any]]:
    await asyncio.sleep(1)  # Fake data retrieval
    return [{"id": 1, "title": "Lorem Ipsum"}]


async def get_albums() -> List[Dict[any, any]]:
    await asyncio.sleep(1)  # Fake data retrieval
    return [{"id": 1, "title": "Mona Lisa"}]


async def load_contents() -> None:
    start_time = time.perf_counter()
    await asyncio.gather(
        get_posts(),
        get_albums(),
    )
    exec_time = (time.perf_counter() - start_time)
    print(f"Execution time: {exec_time:0.2f} seconds.")


async def load_contents_synchronous() -> None:
    start_time = time.perf_counter()
    await get_posts()
    await get_albums()
    exec_time = (time.perf_counter() - start_time)
    print(f"Execution time: {exec_time:0.2f} seconds.")


async def load_contents_using_tasks(self) -> None:
    start_time = time.perf_counter()
    content_types = ["posts", "albums"]
    tasks: List[asyncio.Task] = []
    for content in content_types:
        tasks.append(getattr(self, f"get_{content}")())
    await asyncio.gather(*tasks)
    exec_time = (time.perf_counter() - start_time)
    print(f"Execution time: {exec_time:0.2f} seconds.")


async def app() -> None:
    await load_contents()
    await load_contents_using_tasks()
    await load_contents_synchronous()


if __name__ == "__main__":
    asyncio.run(app())
