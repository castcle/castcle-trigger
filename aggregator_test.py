import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from bson import regex
from datetime import datetime, timedelta
import math

## assign databases
appDb = mongo_client['app-db']
analyticsDb = mongo_client['analytics-db']

## assign collections
### source collections
contents = appDb['contents']

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
        # define cursor to get top creator user Ids
        topCreators = [
            {
                # sort creator users by score
                '$sort': {
                    'score': -1
                }
            }, {
                # slice for only top creator users
                '$limit': topCreatorsLimit
            }, {
                # summarize creator user IDs together
                '$group': {
                    '_id': None, 
                    'creatorList': {
                        '$push': '$_id'
                    }
                }
            }
        ]

        # perform aggregate top creator users then keep them in variable
        topCreatorList = list(creatorStats.aggregate(topCreators))[0]['creatorList']

        # define cursor to get contents from top creator users
        topCreatorContents = [
            {
                # filter for only contents created by top creators
                '$match': {
                    'author.id': {
                        '$in': topCreatorList
                    }
                }
            }, {
                # summarize content IDs together
                '$group': {
                    '_id': None,
                    'contents': {
                '$push': '$_id'
                }
                }
            }, {
                # map format to return content IDs
                '$project': {
                    '_id':'global',
                    'reasonTopCreator': '$contents'
                }    
            }, {
                # upsert to 'aggregatedPool' collection
                '$merge': {
                    'into': {
                        'db': 'analytics-db', 
                        'coll': 'aggregatedPool'
                    }, 
                    'on': '_id', 
                    'whenMatched': "merge",  
                    'whenNotMatched': 'insert'
                }
            }
        ]

        # perform aggregate contents which are created by top creator users
        # upsert them in collection: 'aggregatedPool', document: 'global'
        contents.aggregate(topCreatorContents)

        # define cursor to get top hashtags content Ids (workaroung while waiting field: 'hashtags' in collection: 'contents')
        topHashtagCursor = [
            {
                # order by score
                '$sort': {
                    'score': -1
                }
            }, {
                # slice for top numbers
                '$limit': topHashtaglimit
            }, {
                # deconstruct array to object for grouping
                '$unwind': {
                    'path': '$aggregator.contributions', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                # summarize all documents into a single array
                '$group': {
                    '_id': None, 
                    'contents': {
                        '$push': '$aggregator.contributions.contents'
                    }
                }
            }, {
                # project deconstruct all contentId into a sigle document with label reason as topHashtags
                '$project': {
                    '_id': 'global',
                    'reasonTopHashtags': {
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
            }, {
                # upsert to 'aggregatedPool' collectionb
                '$merge': {
                    'into': {
                        'db': 'analytics-db', 
                        'coll': 'aggregatedPool'
                    }, 
                    'on': '_id', 
                    'whenMatched': 'merge', 
                    'whenNotMatched': 'insert'
                }
            }
        ]

        # ! (workaround)
        # perform aggregate contents which are members of top hashtags
        # upsert them in collection: 'aggregatedPool', document: 'global'
        hashtagStats.aggregate(topHashtagCursor)

        # define cursor to get top contents Ids
        topContentsCursor = [
            {
                # order by score
                '$sort': {
                    'score': -1
                }
            }, {
                # slice for top numbers
                '$limit': topContentslimit
            }, {
                # summarize all documents into a single array
                '$group': {
                    '_id': None, 
                    'contents': {
                        '$push': '$_id'
                    }
                }
            }, {
                # project deconstruct all contentId into a sigle document with label reason as topHashtags
                '$project': {
                    '_id': 'global', 
                    'reasonTopContents': '$contents'
                }
            }, {
                # upsert to 'aggregatedPool' collectionb
                '$merge': {
                    'into': {
                        'db': 'analytics-db', 
                        'coll': 'aggregatedPool'
                    }, 
                    'on': '_id', 
                    'whenMatched': 'merge', 
                    'whenNotMatched': 'insert'
                }
            }
        ]

        # ! (for testing)
        # perform aggregate contents which are top contents
        # upsert them in collection: 'aggregatedPool', document: 'global'
        contentStats.aggregate(topContentsCursor)

        # print message on complete aggregation
        print('global aggregated pool has been updated')
        print('this aggregation has completed at', datetime.utcnow())

    except Exception as error:
        print("ERROR", error)