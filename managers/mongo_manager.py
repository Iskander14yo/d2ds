import logging

import pymongo.errors
from pymongo import MongoClient


class MongoManager:
    def __init__(
            self,
            host: str = "localhost",
            port: int = 27017,
            db_name: str = "dota_db",
    ):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.client = None
        self.db = None
        self.match_id_collection = None
        self.match_info_collection = None
        self.full_match_collection = None
        self._create_connection()

    def _create_connection(self) -> None:
        try:
            self.client = MongoClient(self.host, self.port)
            self.db = self.client[self.db_name]
            self._initialize_collections()
        except pymongo.errors.ServerSelectionTimeoutError as err:
            logging.error(err)

    def _initialize_collections(self) -> None:
        # Initialize your collections here
        self.match_id_collection = self.db['match_id_collection']
        self.match_info_collection = self.db['match_info_collection']
        self.full_match_collection = self.db['full_match_collection']
