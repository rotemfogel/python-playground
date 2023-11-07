from abc import ABC


class _ABS(ABC):
    def __init__(self, name: str):
        self._name = name

    def get_name(self):
        return type(self).__name__ + ": " + self._name


class _AB(_ABS, ABC):
    def __init__(self, name: str):
        super().__init__(name=name)


class A(_AB):
    def __init__(self, name: str):
        super().__init__(name=name)


class B(_AB):
    def __init__(self, fname, lname):
        super().__init__(name=" ".join([fname, lname]))


if __name__ == "__main__":
    b = B("rotem", "fogel")
    print(b.get_name())
    a = A(name="rotem")
    print(a.get_name())
