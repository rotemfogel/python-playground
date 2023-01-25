import re


def camel_to_snake(camel: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", camel).lower()


def snake_to_camel(snake: str) -> str:
    """
    We capitalize the first letter of each component except the first one
    with the 'title' method and join them together.

    :param snake: the snake str
    :type snake: str
    :return: str
    """
    components = snake.split("_")
    return components[0] + "".join(x.title() for x in components[1:])
