from typing import Any


class Variable:
    __NO_DEFAULT = object()

    def __init__(self):
        pass

    @classmethod
    def get(cls,
            key: str,
            default_var: Any = __NO_DEFAULT,
            deserialize_json: bool = False, ) -> Any:
        return {'key': key,
                'value': default_var if default_var is not cls.__NO_DEFAULT else None,
                'serialized': deserialize_json}


def get_var(key: str, deserialize_json: bool = False, default_var: Any = None) -> str:
    if default_var:
        return Variable.get(key,
                            default_var=default_var,
                            deserialize_json=deserialize_json)
    return Variable.get(key,
                        deserialize_json=deserialize_json)


if __name__ == "__main__":
    print(get_var('a'))
    print(get_var('a', default_var='A'))
    print(get_var('b', default_var='B', deserialize_json=True))
