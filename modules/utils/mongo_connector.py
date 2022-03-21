# version 1.0.0
# Licensed: Castcle
# Author: Atitawat Pol-in

from pymongo import MongoClient
import os
import pprint

class MongoDB(MongoClient):
    def __init__(self):
        self.mongo_host = os.environ.get('MONGO_HOST')
        self.mongo_user = os.environ.get('MONGO_USER')
        self.mongo_password = os.environ.get('MONGO_PASSWORD')
        self.mongo_uri = f'mongodb+srv://{self.mongo_user}:{self.mongo_password}@{self.mongo_host}'
        self.mongo_client = MongoClient(self.mongo_uri)
        
    def mongo_find(self, db_name: str, collection_name: str, query_statement: dict):
        db_obj = self.mongo_client[db_name][collection_name]
        db_obj.find(query_statement)
        return db_obj

    def mongo_distinct(self, db_name: str, collection_name: str, distinct_field: str):
        return pprint.pprint(self.mongo_client[db_name][collection_name].distinct(distinct_field))