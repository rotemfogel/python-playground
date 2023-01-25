import json
from typing import Any

from airfart.utils.remove_empty_elements import remove_empty_elements


def to_json(data: Any) -> str:
    return (
        json.dumps(data, default=lambda o: remove_empty_elements(o).__dict__)
        if data
        else "[]"
    )
