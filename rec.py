from typing import List


def count_char(s: str, c: int) -> int:
    def counter(xs: List[str], acc: int) -> int:
        if len(xs) == 0:
            return acc
        it = iter(xs)
        n: str = next(it)
        return counter(list(it), acc + 1 if n == c else acc)

    return counter(list(s.encode()), 0)


if __name__ == "__main__":
    st: str = "ss ccbbb ddsss obnsd kljsn dg"
    assert (count_char(st, ord(' ')) == 5)
    assert (count_char(st, ord('k')) == 1)
    assert (count_char(st, ord('b')) == 4)
    assert (count_char(st, ord('s')) == 7)
    assert (count_char("", ord('q')) == 0)
