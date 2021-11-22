from typing import List, Optional


def count_char(s: str, c: int) -> int:
    def counter(xs: List[str], acc: int) -> int:
        if len(xs) == 0:
            return acc
        it = iter(xs)
        head: str = next(it)
        return counter(list(it), acc + 1 if head == c else acc)

    return counter(list(s.encode()), 0)


def fill_none(xs: List[Optional[int]]) -> List[int]:
    def fill(o: List[Optional[int]], n: List[int], last: Optional[int]) -> List[int]:
        if len(o) == 0:
            return n

        it = iter(o)
        head: Optional[int] = next(it)
        return fill(list(it), n + [last if not head else head], last if not head else head)

    if not xs[0]:
        raise AssertionError('first value must be present !')
    return fill(xs, list(), None)


if __name__ == "__main__":
    st: str = "ss ccbbb ddsss obnsd kljsn dg"
    assert (count_char(st, ord(' ')) == 5)
    assert (count_char(st, ord('k')) == 1)
    assert (count_char(st, ord('b')) == 4)
    assert (count_char(st, ord('s')) == 7)
    assert (count_char("", ord('q')) == 0)

    result = fill_none([1, None, 2, 3, None, None, 4])
    assert (result == [1, 1, 2, 3, 3, 3, 4])
    # negative test
    try:
        fill_none([None, 1, 2, 3])
    except AssertionError as e:
        assert (str(e) == 'first value must be present !')
