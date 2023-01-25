import itertools
import random

colors = ["red", "green", "purple"]
shapes = ["diamond", "squiggle", "oval"]
numbers = ["1", "2", "3"]
shades = ["solid", "striped", "open"]


def check_property(cards, prop):
    # property indexes are: 0 - color, 1 - shape, 2 - number, 3 - shade
    if (cards[0][prop] == cards[1][prop]) & (cards[0][prop] == cards[2][prop]):
        return True
    elif (
        (cards[0][prop] != cards[1][prop])
        & (cards[0][prop] != cards[2][prop])
        & (cards[1][prop] != cards[2][prop])
    ):
        return True
    return False


if __name__ == "__main__":
    temp = [colors, shapes, numbers, shades]
    all_cards = list(itertools.product(*temp))

    while len(all_cards) >= 3:
        three_cards = []
        for i in range(0, 3):
            index = random.randint(0, len(all_cards) - 1)
            temp_card = all_cards[index]
            del all_cards[index]
            three_cards.append(temp_card)
        # property indexes are: 0 - color, 1 - shape, 2 - number, 3 - shade
        if (
            check_property(three_cards, 0)
            & check_property(three_cards, 1)
            & check_property(three_cards, 2)
            & check_property(three_cards, 3)
        ):
            print("You Have A Set!!! \n" + str(three_cards))
            break
        else:
            pass
