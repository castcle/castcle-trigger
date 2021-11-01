# This file aggregates to filter 

import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from datetime import datetime, timedelta
import math

## assign databases
appDb = mongo_client['app-db']
analyticsDb = mongo_client['analytics-db']

## assign collections
### source collections
contents = appDb['contents']
engagements = appDb['engagements']

### destination collections
creatorStats = analyticsDb['creatorStats']
hashtagStats = analyticsDb['hashtagStats']
contentStats = analyticsDb['contentStats']

def handle(event, context):
    print(json.dumps(event, indent=4))

    # define creator parameters
    topCreatorsLimit = 10   

    # define hashtag parameters
    topHashtaglimit = 5

    # define content parameters
    topContentslimit = 100

    try:

        #################################################################
        #################################################################
        # update user engagement by pairing with creator's content
cursor = [
    {
        # summarize by pairing of user ID & content ID 
        '$group': {
            '_id': {
                'userId': '$user', 
                'contentId': '$_id'
            },  
            'engangements': {
                '$push': '$type'
            }
        }
    }, {
        # deconstruct for ease of adding fields
        '$unwind': {
            'path': '$engangements'
        }
    }, {
        # add fields by matching engagement types 
        '$addFields': {
            'like': {
                '$eq': [
                    '$engangements', 'like'
                ]
            }, 
            'comment': {
                '$eq': [
                    '$engangements', 'comment'
                ]
            }, 
            'recast': {
                '$eq': [
                    '$engangements', 'recast'
                ]
            }, 
            'quote': {
                '$eq': [
                    '$engangements', 'quote'
                ]
            }
        }
    }, {
        # summarize to merge all added engagement types
        '$group': {
            '_id': '$_id', 
            'like': {
                '$first': '$like'
            }, 
            'comment': {
                '$first': '$comment'
            }, 
            'recast': {
                '$first': '$recast'
            }, 
            'quote': {
                '$first': '$quote'
            }
        }
    }, {
        # map output format as followed requirement
        '$project': {
            '_id': 0, 
            'userId': '$_id.userId', 
            'contentId': '$_id.contentId', 
            'like': '$like', 
            'comment': '$comment', 
            'recast': '$recast', 
            'quote': '$quote'
        }
    }, {
        # upsert to 'transactionEngagements' collectionb
        '$merge': {
            'into': {
                'db': 'analytics-db', 
                'coll': 'transactionEngagements'
            }, 
            'on': '_id', 
            'whenMatched': 'replace', 
            'whenNotMatched': 'insert'
        }
    }
]


        # print message on complete aggregation
        print('global aggregated pool has been updated')
        print('this aggregation has completed at', datetime.utcnow())
        
        # print output
        print(list(engagements.aggregate(cursor)))

    except Exception as error:
        print("ERROR", error)