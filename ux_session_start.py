import json
from mongo_client import mongo_client

db = mongo_client['analytics-db']


def handle(event, context):
    print(json.dumps(event, indent=4))

    # event.fullDocument._id
# have to specify 'event'
