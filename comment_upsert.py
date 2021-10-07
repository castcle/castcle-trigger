# This file is updating hashtag statistics among recent contents
import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from datetime import datetime, timedelta

# setup databases & collections
appDb = mongo_client['app-db']
analyticsDb = mongo_client['analytics-db']
contents = appDb['contents']
hashtagStats = analyticsDb['hashtagStats']


def handle(event, context):
    print(json.dumps(event, indent=4))

    # define cursor
    updateHashtagCursor = [
    {
        # filter for new than 14 days contents
        '$match': {
            'createdAt': {
                '$gte': (datetime.now() - timedelta(days=14)) 
                }
        }
    }, {
        # deconstruct array hashtags into separate documents
        '$unwind': {
            'path': '$payload.message', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        # summarize by user (not account)
        '$group': {
            '_id': {
                'hashtag': '$payload.message', 
                'authorId': '$author.id'
            }, 
            'contribution': {
                '$count': {}
            }, 
            'createdAt': {
                '$min': '$createdAt'
            }, 
            'updatedAt': {
                '$max': '$updatedAt'
            }
        }
    }, {
        # summarize by hashtag
        '$group': {
            '_id': '$_id.hashtag', 
            'hashtagCount': {
                '$count': {}
            }, 
            'createdAt': {
                '$min': '$createdAt'
            }, 
            'updatedAt': {
                '$max': '$updatedAt'
            }, 
            'contributorsDetail': {
                '$push': {
                    '_id': '$_id.authorId', 
                    'contribution': '$contribution', 
                    'updatedAt': '$updatedAt'
                }
            }
        }
    }, {
        # setting format
        '$project': {
            '_id': 0, 
            'hashtag': '$_id', 
            'hashtagCount': 1, 
            'createdAt': 1, 
            'updatedAt': 1, 
            'contributorsDetail': 1
        }
    }, {
        # upsert to 'hashtagStats' collection
        '$merge': {
            'into': {
                'db': 'analytics-db', 
                'coll': 'hashtagStats'
            }, 
            'on': '_id', 
            'whenMatched': 'replace', 
            'whenNotMatched': 'insert'
         }
        }
    ]

    try:
        # perform aggregation w/ resulting in upsert 'userStats' collection
        contents.aggregate(updateHashtagCursor)

        # print message on complete aggregation
        print('this aggregation has completed at', datetime.now())

    except ERROR as error:
        print("ERROR", error)
