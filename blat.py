from typing import Dict, Set, List


def most_tips(user_ids_: List[int], tips_: List[int]) -> int:
    if len(user_ids_) != len(tips_):
        raise ValueError("lists length must be the same")
    user_tips: Dict[int, int] = dict()
    for i, user in enumerate(user_ids_):
        user_tips[user] = user_tips.get(user, 0) + tips_[i]
    return sorted(user_tips.items(), key=lambda k: k[1])[-1]


def search_missing_number(list_num: List[int]) -> int:
    n = len(list_num)
    # checks
    if list_num[0] != 1:
        return 1
    if list_num[n - 1] != (n + 1):
        return n + 1
    total = (n + 1) * (n + 2) / 2
    sum_of_list = sum(list_num)
    return int(total - sum_of_list)


if __name__ == "__main__":
    user_ids = [103, 105, 105, 107, 106, 103, 102, 108, 107, 103, 102]
    tips = [2, 5, 1, 0, 2, 1, 1, 0, 0, 2, 2]
    user_id = most_tips(user_ids, tips)
    print(user_id[0])
    print(search_missing_number([1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12]))
