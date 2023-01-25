import collections
import math
import time


def ensure_int32(val):
    import sys

    min_size, max_size = -sys.maxsize - 1, sys.maxsize
    if not min_size < val < max_size:
        raise ValueError(f"product is not in int32 range: {val=}")
    return val


def do_list_multiply_foreach_handling_zeros(arr):
    retval = 1
    for i in arr:
        if i == 0:
            continue
        retval *= i
    return retval


# sic! extremely ineffective and slow way to divide, more or less effective on small arrays with small numbers
# other methods like bitwise & Goldschmidt etc. TLDR
def divide_euclid(dividend, divisor):
    if divisor == 0:
        raise ZeroDivisionError("Check your client code to handle zeros!")
    dividend_sign, divisor_sign = int(math.copysign(1, dividend)), int(
        math.copysign(1, divisor)
    )
    dividend, divisor = abs(dividend), abs(divisor)
    quotient = 0
    while dividend >= divisor:
        dividend -= divisor
        quotient += 1

    return int(math.copysign(quotient, dividend_sign * divisor_sign))


# kind of hack to not use the division or complex division algorithms
def divide_reverse(dividend, divisor):
    if divisor == 0:
        raise ZeroDivisionError("Check your client code to handle zeros!")
    return int(dividend * divisor**-1)


def assert_result(r):
    print(r)
    assert r == [15, 10, 6, 30]


if __name__ == "__main__":
    array = [2, 3, 5, 1]
    zeros_cnt = collections.Counter(array)[0]
    result = [0] * len(array)
    mult_result = do_list_multiply_foreach_handling_zeros(array)  # O(n)

    for pos, val in enumerate(array):  # O(n)
        # not sophisticated switch-case part because of zeros handling
        if len(array) <= 1:
            result = array
            break
        elif zeros_cnt > 1:
            break
        elif zeros_cnt == 1 and val != 0:
            continue
        elif zeros_cnt == 1 and val == 0:
            result[pos] = mult_result
        else:
            start = time.time()
            try:
                result[pos] = ensure_int32(divide_reverse(mult_result, val))
                # result[pos](ensure_int32(divide_euclid(mult_result, val)))
                # result[pos](ensure_int32(mult_result // val))
            finally:
                end = time.time()
                print(f"iteration {pos}:", end - start, "sec")

    assert_result(result)
    ### Version for O(n^2) & avoid handling zeros routine
    result = []
    for pos, val in enumerate(array):
        tmp_list = array.copy()
        tmp_list.pop(pos)
        result.append(do_list_multiply_foreach_handling_zeros(tmp_list))

    assert_result(result)
    # General complexity -> O(n)
