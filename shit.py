def foo(l: list[int | str]) -> list[int | str]:
    return [i * 2 if type(i) == int else f"{i}{i}" for i in l]


if __name__ == "__main__":
    ints = list(range(1, 4))
    chars = list(map(lambda x: ascii(x), ints))
    print(foo(ints))
    print(foo(chars))
