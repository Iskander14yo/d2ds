from typing import Any

import pymongo.errors
from pymongo import MongoClient

from parsing.utils import logger


def create_mongo_connection() -> tuple[MongoClient, Any]:
    """
    Соединение монго, можете это не трогать,
    только измените локалку, если у вас порт закрыт
    """
    try:
        # Менять соединение с монгой
        client = MongoClient("localhost", 27017)
        db = client.dota_db
        return client, db
    except pymongo.errors.ServerSelectionTimeoutError as err:
        logger.error(err)


# Метод для разделения батчей, можно не трогать
def create_batches(lst: list, n: int) -> list:
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
