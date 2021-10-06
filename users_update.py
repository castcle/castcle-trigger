import json
from mongo_client import mongo_client
from datetime import datetime, timedelta

analyticsDb = mongo_client['analytics-db']
contents = analyticsDb ['testContents']

def handle(event, context):
    print(json.dumps(event, indent=4))

print(datetime.now())