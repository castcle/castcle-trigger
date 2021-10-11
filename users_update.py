# This file is not work yet
# ultimate goal is to return contents/contentId w/ reasons as output for 'feedItem' collection
import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from datetime import datetime, timedelta

# setup databases & collections
appDb = mongo_client['app-db']
analyticsDb = mongo_client['analytics-db']
contents = appDb['contents']
creatorStats = analyticsDb['creatorStats']
hashtagStats = analyticsDb['hashtagStats']

def handle(event, context):
    print(json.dumps(event, indent=4))

# define cursor
## now we have 3 effective collections i.e. 'creatorStats', 'hashtagStat', 'contents'
# 1. filter only potential related contents as aggregator
# 2. ordering for obain top potential contents as ranker

    # define cursor
    cursor = [
    {
        # filter for new than 14 days contents
        '$match': {
            'createdAt': {
                '$gte': (datetime.now() - timedelta(days=14)) 
                }
        }
    }

    try:
        # perform aggregation w/ resulting in upsert 'hashtagStats' collection
        contents.aggregate(cursor)

        # print message on complete aggregation
        print('this aggregation has completed at', datetime.now())

    except Exception as error:
        print("ERROR", error)