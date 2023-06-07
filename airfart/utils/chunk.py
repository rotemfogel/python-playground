from typing import Any


def chunk_fn(data: list[Any], n: int) -> list[list[Any]]:
    return [data[i * n : (i + 1) * n] for i in range((len(data) + n - 1) // n)]
