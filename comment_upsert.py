# This file updates collection 'hashtagStats' from 'contents'
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
    cursor = [
    {
        # filter for new than 14 days contents
        '$match': {
            'createdAt': {
                '$gte': (datetime.now() - timedelta(days=14)) 
                }
        }
    }, {
        # extract hashtags => array
        '$addFields': {
            'hashtags': {
                '$regexFindAll': {
                    'input': '$payload.message', 
                    'regex': re.compile(r"#(\\w+)"), 
                    'options': 'i'
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
                '$toLower': {
                    '$arrayElemAt': [
                        '$hashtags.captures', 0
                    ]
                }
            }
        }
    }, {
        # summarize by user (not account)
        '$group': {
            '_id': {
                'hashtag': '$hashtag', 
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
                '$sum': '$contribution'
            }, 
            'contributorCount': {'$count': {}},
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
            'contributorCount': 1,
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
        # perform aggregation w/ resulting in upsert 'hashtagStats' collection
        contents.aggregate(cursor)

        # print message on complete aggregation
        print('this aggregation has completed at', datetime.now())

    except ERROR as error:
        print("ERROR", error)
