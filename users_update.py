# This file is not work yet
# aim to return output for 'feedItem' collection
import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from datetime import datetime, timedelta

# setup databases & collections
appDb = mongo_client['app-db']
analyticsDb = mongo_client['analytics-db']
contents = appDb['contents']
userStats = analyticsDb['userStats']
hashtagStats = analyticsDb['hashtagStats']

def handle(event, context):
    print(json.dumps(event, indent=4))

# define cursor