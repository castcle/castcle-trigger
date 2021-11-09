# this file aims to implement aggregations whenevery ux_session is started
# type: database trigger

import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from bson import regex
from datetime import datetime, timedelta
import math

# assign databases
appDb = mongo_client['app-db']


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        print("WarmUp - Lambda is warm!")
        return

    print(json.dumps(event, indent=4))

    # event.fullDocument._id
    # have to specify 'event'
    '''
    print(event) ->
    {'accountid': "ObjectId('61700a6151db852fd36d2142')"}
    '''
    try:
        print('event is')
        print(event)
        print('\n')

        print('detail is')
        print(event['detail'])
        print('\n')

        print('document is')
        print(event['detail']['fullDocument'])
        print('\n')

        if event['detail']['operationType'] == "insert":
            print("hooray")

    except Exception as error:
        print("ERROR", error)
