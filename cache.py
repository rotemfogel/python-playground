import json
import os
import pickle
from typing import List, Any, Optional, Set
from urllib.parse import urlparse

import redis
from dotenv import load_dotenv

load_dotenv()

__CACHE_VERSION = 'V1'
__CACHE_KEY_DELIMITER = '-'
__REDIS_URL = os.getenv('REDIS_URL') or 'rediss://127.0.0.1:6381'
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
    return [key.decode('utf-8') for key in __REDIS_CACHE.keys()]


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


if __name__ == '__main__':
    cache_keys = cache_keys()
    key_sizes = list(map(lambda x: {len(x): x}, cache_keys))
    # for cache_key in cache_keys:
    #     lk = len(cache_key)
    #     count = key_sizes.get(lk, 0) + 1
    #     key_sizes.update({lk: count})

    from pprint import pprint
    print(json.dumps(key_sizes, indent=1))
