import json
import os
import pickle
from typing import Any, List, Optional, Set
from urllib.parse import urlparse

import redis
import yaml
from dotenv import load_dotenv

load_dotenv()

__CACHE_VERSION = "V1"
__CACHE_KEY_DELIMITER = "-"
__REDIS_URL = os.getenv("REDIS_URL") or "rediss://127.0.0.1:6381"
__URL = urlparse(__REDIS_URL)
__REDIS_CACHE = redis.Redis(
    host=__URL.hostname,  # type: ignore
    port=__URL.port,  # type: ignore
    username=__URL.username,
    password=__URL.password,
    ssl=True,
    ssl_cert_reqs=None,
)


def cache_keys() -> List[str]:
    return [key.decode("utf-8") for key in __REDIS_CACHE.keys()]


def cache_set(
    key: str, value: Any, ttl: int | None = None, do_pickle: bool = True
) -> None:
    if value is not None:
        value_ = pickle.dumps(value, fix_imports=True) if do_pickle else value
        __REDIS_CACHE.set(key, value_)
        if ttl:
            __REDIS_CACHE.expire(key, ttl)


def cache_get(
    key: str, ttl: int | None = None, auto_renew: bool = False, unpickle: bool = True
) -> Optional[Any]:
    result = __REDIS_CACHE.get(key)
    if result:
        if auto_renew and ttl is not None and ttl > 0:
            __REDIS_CACHE.expire(key, ttl)
    return pickle.loads(result) if result and unpickle else result


def cache_set_get(key: str) -> Set[Any]:
    key_ = __CACHE_KEY_DELIMITER.join([__CACHE_VERSION, key])
    result = __REDIS_CACHE.smembers(key_)
    return set([pickle.loads(s) for s in result])


def cache_set_add(key: str, value: Any) -> None:
    key_ = __CACHE_KEY_DELIMITER.join([__CACHE_VERSION, key])
    value_ = pickle.dumps(value)
    __REDIS_CACHE.sadd(key_, value_)


def cache_set_remove(key: str, value: Any) -> None:
    key_ = __CACHE_KEY_DELIMITER.join([__CACHE_VERSION, key])
    value_ = pickle.dumps(value)
    __REDIS_CACHE.srem(key_, value_)


def cache_set_lookup(key: str, value: Any) -> bool:
    return value in cache_set_get(key)


def get_cache_keys() -> List[str]:
    file = "cache_keys.pkl"
    try:
        with open(file, "rb") as r:
            keys = pickle.load(r)
    except IOError:
        keys = cache_keys()
        with open(file, "wb") as w:
            pickle.dump(keys, w)
    return keys


if __name__ == "__main__":
    key_sizes_file = "key_sizes.yml"
    cache_keys = get_cache_keys()
    cache_key_sizes = list(map(lambda x: {len(x): x}, cache_keys))
    key_sizes = {}
    with open(key_sizes_file, "r") as f:
        key_sizes = yaml.safe_load(f)
    if not key_sizes:
        key_sizes = {}
        for cache_key in cache_key_sizes:
            key_size = list(cache_key.keys())[0]
            count = key_sizes.get(key_size, 0) + 1
            key_sizes.update({key_size: count})
        with open(key_sizes_file, "w") as f:
            yaml.safe_dump(key_sizes, f)
    large_keys_sz = list(filter(lambda k: k > 5000, key_sizes.keys()))
    large_keys = []
    for k_sz in large_keys_sz:
        for i in cache_key_sizes:
            if list(i.keys())[0] == k_sz:
                large_keys.append(i[k_sz])
    with open("large_keys.json", "w") as f:
        json.dump(large_keys, f, indent=1)
