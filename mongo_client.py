import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi

local = 'local'
mongo_uri = 'mongodb+srv://analytics-user:cAqQsy9RceXU53eo@dev-cluster.fg2e5.mongodb.net/?retryWrites=true&w=majority'
# mongo_uri = 'mongodb://localhost:27017'
env = os.environ.get('ENV') or local

print(f'ENV={env}')

if env != local:
    mongo_host = os.environ.get('MONGO_HOST')
    mongo_password = os.environ.get('MONGO_PASSWORD')
    mongo_uri = f'mongodb+srv://analytics-user:{mongo_password}@{mongo_host}/?retryWrites=true&w=majority'

print(f'mongo_uri={mongo_uri}')

mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
