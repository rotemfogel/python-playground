import timeit


def memoize(func):
    cache = dict()

    def memoized_func(*args):
        if args in cache:
            return cache[args]
        result = func(*args)
        cache[args] = result
        return result

    return memoized_func


def fibonacci(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    return fibonacci(n - 1) + fibonacci(n - 2)


if __name__ == "__main__":
    memoized_fibonacci = memoize(fibonacci)
    print(timeit.timeit('fibonacci(35)', globals=globals(), number=1))
    print(timeit.timeit('fibonacci(35)', globals=globals(), number=1))
    print(timeit.timeit('fibonacci(35)', globals=globals(), number=1))
    print(timeit.timeit('memoized_fibonacci(35)', globals=globals(), number=1))
    print(timeit.timeit('memoized_fibonacci(35)', globals=globals(), number=1))
    print(timeit.timeit('memoized_fibonacci(35)', globals=globals(), number=1))
