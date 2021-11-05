import json
from mongo_client import mongo_client

db = mongo_client['analytics-db']

def handle(event, context):
    
    return