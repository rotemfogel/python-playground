from dataclasses import dataclass, fields
from datetime import date
from typing import Optional


@dataclass
class Person:
    id: int
    name: str
    dbo: Optional[date] = None

    def __repr__(self):
        return ";".join(
            [
                f"{f.name}:{self.__getattribute__(f.name)}"
                for f in fields(self)
                if self.__getattribute__(f.name)
            ]
        )


if __name__ == "__main__":
    john = Person(1, "John", date(1975, 6, 30))
    print(john)
    jane = Person(1, "Jane")
    print(jane)
