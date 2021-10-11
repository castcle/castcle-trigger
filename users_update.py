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

    # define finding cursors
    findCursor = {
        "updatedAt": {
            '$gte': (datetime.now() - timedelta(days=hashtagDateThreshold))
        }
    }

    # define projection cursors
    projectCursor = {
        '_id': 1
    }

    # project sorting cursor
    sortCursor = [
        ("hashtagCount", -1)
    ]

    topHashtagList = list(hashtagStats.find(findCursor, projectCursor)
            .sort(sortCursor)
            .limit(hashtagThreshold))

    topHashtags = [hashtag[key] for hashtag in topHashtagList for key in hashtag]
    
    #################################################################
    # aggregate contents from found hashtags
    #################################################################
    # define RegEx
    pattern = regex.Regex.from_native(re.compile(r"(?<=#)\w+"))
    pattern.flags ^= re.UNICODE
    
    # define content parameters
    contentDateThreshold = 14

    # define cursor
    cursor = [
        {
            # filter for new than 14 days contents
            '$match': {
                'createdAt': {
                    '$gte': (datetime.now() - timedelta(days=contentDateThreshold)) 
                    }
            }
        }, {
            # extract hashtags => array
            '$addFields': {
                'hashtags': {
                    '$regexFindAll': {
                        'input': '$payload.message', 
                        'regex': pattern
                    }
                }
            }
        }, {
            # deconstruct hashtags array => hashtag object
            '$unwind': {
                'path': '$hashtags', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            # extract hashtag object => field
            '$addFields': {
            'hashtag': {
                '$toLower': '$hashtags.match'
                }
            }
        }, {

        }

    try:
        # perform aggregation w/ resulting in upsert 'hashtagStats' collection
        contents.aggregate(cursor)

        # print message on complete aggregation
        print('this aggregation has completed at', datetime.now())

    except Exception as error:
        print("ERROR", error)