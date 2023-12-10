import asyncio
import datetime
import time
import traceback

import aiohttp
import pymongo.errors
import requests
from pymongo import UpdateOne

from managers.mongo_manager import MongoManager
from parsing.config import (BATCH_SIZE, FIRST_RUN, LESS_THEN_MATCH,
                            LOCAL_SPREAD_BOT, LOCAL_SPREAD_TOP,
                            LOCAL_STRATZ_TOKEN, OPENDOTA_URL,
                            REMAIN_OPENDOTA_REQUESTS, REMAIN_STRATZ_REQUESTS,
                            STRATZ_URL)
from parsing.stratz_query import query
from parsing.utils import create_batches
from utils import logger

mongo_manager = MongoManager()
match_ids_coll = mongo_manager.match_id_collection
match_info = mongo_manager.match_info_collection
full_match = mongo_manager.full_match_collection


def opendota_request_match_ids(less_than_match_id: int) -> list[str]:
    # В этот список собираем id
    match_ids = list()
    i = REMAIN_OPENDOTA_REQUESTS

    try:
        while i > 0:
            # Запрашиваем ID в opendota
            req_url = str(
                f"{OPENDOTA_URL}?"
                "less_than_match_id="
                + str(less_than_match_id)
                + "&max_rank="
                + str(LOCAL_SPREAD_TOP)
                + "&min_rank="
                + str(LOCAL_SPREAD_BOT)
            )
            public_matches = requests.get(req_url)  # Шлем запрос - получаем 100 матчей (по идее)
            if public_matches.status_code == 200:  # Проверяем статус запроса
                matches_data = public_matches.json()
                match_info.insert_many(matches_data)
                logger.info(f"Request [{i}] successful; "
                            f"{len(matches_data)} matches processed.")

                match_ids.extend((k["match_id"] for k in matches_data))
                less_than_match_id = match_ids[-1]

                time.sleep(1)
                i -= 1
            else:
                logger.warning(f"Request [{i}] failed; "
                               f"status code {public_matches.status_code}.")

    except requests.ConnectionError as e:
        logger.error("Connection error occurred in opendota_request_match_ids method: %s", e)
    except requests.RequestException as e:
        logger.error("Request error occurred in opendota_request_match_ids method: %s", e)
    except pymongo.errors.ConnectionFailure as e:
        logger.error(
            "PyMongo connection error occurred in opendota_request_match_ids method: %s", e
        )
    except pymongo.errors.BulkWriteError as e:
        logger.error("Inserting data error occurred in opendota_request_match_ids method: %s", e)
    except Exception as e:
        logger.error("Something went wrong in opendota_request_match_ids method: %s", e)

    return match_ids


async def insert_match_ids_into_db(match_ids: list[str]) -> bool:
    try:
        # Формируем айдишники для бд
        if FIRST_RUN:
            insert_ids = [
                {
                    "id": m_id,
                    "insert_time": datetime.datetime.now(tz=datetime.timezone.utc)
                    + datetime.timedelta(hours=3, days=-1),
                }
                for m_id in match_ids
            ]
        else:
            insert_ids = [
                {
                    "id": m_id,
                    "insert_time": datetime.datetime.now(tz=datetime.timezone.utc)
                    + datetime.timedelta(hours=3),
                }
                for m_id in match_ids
            ]

        # Вносим айдищники в бд через update
        for i in insert_ids:
            update = {"$set": i}
            result_ids = match_ids_coll.update_one(i, update, upsert=True)

            # Проверяем на успех
            if result_ids is not None:
                logger.info(
                    f"match[id] = {i.get('id')} "
                    f"successfully inserted into db: @match_id_collection"
                )
            else:
                logger.error(f"match[id] = {i.get('id')} insertion error occurred")

    except pymongo.errors.BulkWriteError as e:
        logger.error("Incorrect data insertion into db @match_id_collection: %s", e)
    except Exception as e:
        logger.error("Что то пошло не так при внесении в бд айди матчей: %s", e)
        return False
    return True


def get_yesterday_matches_from_mongo() -> list[str]:
    try:
        mongo_manager.client.admin.command("ping")
        # Если вы один день не парсили матчи, поменяйте days на 2
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1, hours=3)
        # для запроса: ниже получаем данные из бд между вчера и сегодня
        today = datetime.datetime.now()

        result = match_ids_coll.find({"insert_time": {"$gte": yesterday, "$lt": today}})

        if result is not None:
            logger.info("match ids successfully received from @match_id_collection")
        # Отбираем только айдишники
        m_ids = [_id.get("id") for _id in result]
        return m_ids

    except pymongo.errors.ConnectionFailure as e:
        logger.error("Cant connect to mongodb collection: @match_id_collection: %s", e)
    except pymongo.errors.InvalidOperation as e:
        logger.error("Invalid mongodb operation: %s", e)
    except Exception as e:
        logger.error("An error occurred in get_yesterday_matches_from_mongo method: %s", e)


def create_queries(match_id_batch):
    """Создание батча из queries для запроса в stratz"""
    query_batch = [{"query": query % _id} for _id in match_id_batch]
    return query_batch


async def get_stratz_data(match_ids: list[str]):
    batch_index = 0
    stratz_index = 0
    # батч из айдишников, по умолчанию - список из 5 айдишников
    ids_batch = list(create_batches(match_ids, BATCH_SIZE))

    try:
        # цикл для получения всех запросов из стратза
        while (stratz_index < REMAIN_STRATZ_REQUESTS) & (batch_index < len(ids_batch)):
            start = datetime.datetime.now()  # Засекаем время
            queries_batch = create_queries(ids_batch[batch_index])  # Создаем батч из queries
            logger.info("Началась обработка батча")
            matches = await stratz_api_calls(queries_batch)  # Дожидаемся запросов
            if matches is None:  # Проверяем данные
                continue

            updates = []
            print_updates = []
            for match in matches:
                match_data = match["data"]["match"]
                # проверяем конкретный матч
                if match_data is None:
                    logger.info("Match is None")
                    continue
                else:
                    # проверяем матч на то что обработан стратзом
                    if match_data.get("parsedDateTime") is None:
                        logger.info(f"Match[id]: {match_data.get('id')} not parsed by Stratz yet")
                    else:
                        _filter = {"_id": match_data.get("id")}
                        update = {
                            "$set": {
                                "_id": match_data.get("id"),
                                "match": match_data,
                                "insert_date": datetime.datetime.now(),
                            }
                        }
                        updates.append(UpdateOne(_filter, update, upsert=True))
                        print_updates.append(match_data.get("id"))
                        # result = full_match.update_one(_filter, update, upsert=True)
            if len(updates) > 0:
                result = full_match.bulk_write(updates)

                if result is not None:
                    logger.info(
                        f"Batch [{batch_index}] successfully inserted.\n"
                        f"Batches remain: {str(len(ids_batch) - batch_index - 1)}"
                    )
                    logger.info(print_updates)
                    logger.info(result)
            else:
                logger.error(
                    f"Error occurred while inserting batch: "
                    f"{str(len(ids_batch) - batch_index - 1)}"
                )

            # Перестаем засекать время
            end = datetime.datetime.now()
            # если не прошло 6 секунд, засыпаем чтобы в сумме было 6 секунд
            if (end - start).seconds < 30:
                print()
                time.sleep(30 - (end - start).seconds)
            # увеличиваем индексы пробежки массива
            batch_index += 1
            stratz_index += BATCH_SIZE

    except pymongo.errors.BulkWriteError as e:
        logger.error(e)
    except IndexError as e:
        logger.error(e)
    except Exception:
        logger.error(
            f"Что то пошло не так при отправке стратз запроса "
            f"в бд в батче из следующих матчей: {ids_batch[batch_index]}"
        )
        logger.error(traceback.format_exc())


async def stratz_api_calls(query_batch):
    try:
        async with aiohttp.ClientSession() as session:
            # формируем таски из батчей запросов
            tasks = [asyncio.ensure_future(fetch(session, data)) for data in query_batch]
            # выполняем таски, получая запросы в респонс
            responses = await asyncio.gather(*tasks)
            return responses
    except Exception as e:
        logger.error("Что то пошло не так при парсинге стратз батча: %s", e)


async def fetch(session, data) -> dict:
    """Формируем 1 запрос в stratz"""
    async with session.post(
        STRATZ_URL, json=data, headers={"Authorization": f"Bearer {LOCAL_STRATZ_TOKEN}"}
    ) as response:
        return await response.json()


async def main() -> None:
    if FIRST_RUN:
        # Получаем ID матчей в первый раз
        ids = opendota_request_match_ids(LESS_THEN_MATCH)
        # Отправляем ID и матчи в бд
        await insert_match_ids_into_db(ids)
    else:
        # Получаем вчерашние айдишники
        yesterday_matches = get_yesterday_matches_from_mongo()
        # Получаем ответ от стратза и вносим в бд
        await get_stratz_data(yesterday_matches)
        # Запрашиваем матчи за вчера
        ids = opendota_request_match_ids("")
        # Вносим айдишники матчей в бд
        await insert_match_ids_into_db(ids)


if __name__ == "__main__":
    asyncio.run(main())
