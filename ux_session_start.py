import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from bson import regex
from datetime import datetime, timedelta
import math

## assign databases
appDb = mongo_client['app-db']


def handle(event, context):
    print(json.dumps(event, indent=4))

    # event.fullDocument._id
    # have to specify 'event'

    try:
        print(event)

        print(event.detail)

        print(event.detail.fullDocument)

        if event.detail == "insert":
            print("hooray")

    except Exception as error:
        print("ERROR", error)
