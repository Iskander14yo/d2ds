from typing import TypeVar

T = TypeVar('T')


def create_batches(lst: list[T], n: int) -> list[T]:
    """ Метод для разделения батчей. """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
