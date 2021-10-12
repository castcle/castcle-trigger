# This file is not work yet
# ultimate goal is to return contents/contentId w/ reasons as output for 'feedItem' collection
import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from bson import regex
from datetime import datetime, timedelta
import re

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

    #################################################################
    # find top hashtag #
    #################################################################
    # define query parameters
    hashtagThreshold = 5
    hashtagDateThreshold = 7

    cursor = [
        {
            # filter for new than 14 days contents
            '$match': {
                'createdAt': {
                    '$gte': (datetime.now() - timedelta(days=hashtagDateThreshold)) 
                    }
            }
        }, {
            # filter non-hashtag out to prevent bias
            '$match': {
                '_id': {
                    '$ne': ''
                }
            }
        }, {
            # sort by count
            '$sort': {
                'hashtagCount': -1
            }
        }, {
            # slice for top numbers
            '$limit': hashtagThreshold
        }, {
            # deconstruct 
            '$unwind': {
                'path': '$contributorsDetail'
            }
        }, {
            # group all documents together
            '$group': {
                '_id': None, 
                'contents': {
                    '$push': '$contributorsDetail.contents'
                }
            }
            # project deconstruct all contentId into a sigle document with label reason as topHashtag
        }, {
            '$project': {
                '_id': 'topHashtag', 
                'contentIds': {
                    '$reduce': {
                        'input': '$contents', 
                        'initialValue': [], 
                        'in': {
                            '$concatArrays': [
                                '$$value', '$$this'
                            ]
                        }
                    }
                }
            }
        }
    ]

    try:
        # aggregate then keep results as array of contentIds
        topHashtagContents = list(hashtagStats.aggregate(cursor))[0]['contentIds']

        # print message on complete aggregation
        print('this aggregation has completed at', datetime.now())

        print(topHashtagContents)

    except Exception as error:
        print("ERROR", error)

    #################################################################
    # aggregate contents from found hashtags
    #################################################################
    

    #################################################################