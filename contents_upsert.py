import json
from mongo_client import mongo_client
import pymongo # connect to MongoDB
from datetime import datetime, timedelta
from pprint import pprint
import random

analyticsDb = mongo_client['analytics-db']
contents = analyticsDb ['testContents']

def handle(event, context):
    print(json.dumps(event, indent=4))
    