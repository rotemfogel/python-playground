from abc import ABC
from typing import Dict, Any, Tuple


def generate(data: Dict[str, Any]) -> Tuple[Dict[str, Any]]:
    keys = list(data.keys())
    return ({keys[0]: data[keys[0]]}, {keys[-1]: data[keys[-1]]})


class BaseClass(ABC):
    def __init__(self, foo: Dict[str, Any], bar: Dict[str, Any]):
        self._foo = foo
        self._bar = bar

    def get_foo(self) -> Dict[str, Any]:
        return self._foo

    def get_bar(self) -> Any:
        return self._bar


class AClass(BaseClass):
    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "AClass":
        (foo, bar) = generate(data)
        return cls(foo, bar)

    def __init__(self, foo: Dict[str, Any], bar: Dict[str, Any]):
        super().__init__(foo, bar)


class BClass(BaseClass):
    @classmethod
    def from_data(cls, data: Dict[str, Any], dim: int) -> "BClass":
        (foo, bar) = generate(data)
        return cls(foo, bar, dim)

    def __init__(self, foo: Dict[str, Any], bar: Dict[str, Any], dim: int):
        super().__init__(foo, bar)
        self._dim = dim

    def get_dim(self) -> int:
        return self._dim


if __name__ == "__main__":
    data = dict(foo="foo", bar="bar")
    a_class = AClass.from_data(data)
    b_class = BClass.from_data(data, 1)
    print(a_class.get_foo())
    print(a_class.get_bar())
    print(b_class.get_foo())
    print(b_class.get_bar())
    print(b_class.get_dim())
