import asyncio
import json
import os
import time
from typing import Any, Dict, List

import loguru
import requests
from dotenv import load_dotenv
from requests import Response

from airfart.utils.chunk import chunk_fn

load_dotenv()
BASE_URL = os.getenv("BASE_URL")
REGION = os.getenv("REGION")
BRAND = os.getenv("BRAND")
LOG = loguru.logger


async def fetch(entity_id: str) -> None:
    file_name = f"data/json/{entity_id}.json"
    if not os.path.exists(file_name):
        start_t = time.time()
        url = BASE_URL % entity_id
        response: Response = requests.get(url)
        json_response: Dict[str, Any] = response.json()
        if json_response["status"] == 200:
            with open(file_name, "w") as f:
                json.dump(json_response, f, allow_nan=False, indent=1)
            LOG.info(f"wrote %s.json in %.2fs" % (entity_id, time.time() - start_t))
        else:
            LOG.error("error fetching entity %s" % entity_id)
    else:
        LOG.debug("file %s already written" % file_name)


async def fetch_all() -> None:
    start_t = time.time()
    url = BASE_URL % "list"
    response: Response = requests.get(url)
    LOG.info("fetch all entities in %.2fs" % (time.time() - start_t))
    json_response: Dict[str, Any] = response.json()
    entities: List[str] = json_response["result"]
    concurrency = int(os.cpu_count() * 0.9)
    chunks = chunk_fn(entities, concurrency)
    counter = 0
    for chunk in chunks:
        counter += 1
        start_t = time.time()
        tasks = []
        for elem in chunk:
            tasks.append(fetch(elem))
        await asyncio.gather(*tasks)
        LOG.info("fetch batch %d in %.2fs" % (counter, time.time() - start_t))


if __name__ == "__main__":
    asyncio.run(fetch_all())
