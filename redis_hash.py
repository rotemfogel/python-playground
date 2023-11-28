import redis

USE_OPTIMIZATION = True

_REDIS_CLIENT = redis.Redis(port=6381, ssl=True, ssl_cert_reqs=None)


def hash_get_key_field(key: str) -> dict[str, str]:
    s = key.split(":")
    if len(s[1]) > 2:
        return {"key": s[0] + ":" + s[1][:-3], "field": s[1][-2:]}
    else:
        return {"key": s[0] + ":", "field": s[1]}


def hash_set(key: str, value: str):
    kf = hash_get_key_field(key)
    _REDIS_CLIENT.hset(kf["key"], kf["field"], value)


def hash_get(key: str, value: str):
    kf = hash_get_key_field(key)
    _REDIS_CLIENT.hget(kf["key"], kf["field"], value)


if __name__ == "__main__":
    for i in range(100_001):
        k = "object:" + str(i)
        if USE_OPTIMIZATION:
            hash_set(k, "val")
        else:
            _REDIS_CLIENT.set(k, "val")
