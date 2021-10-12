# This file updates collection 'creatorStats' from 'contents'
import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from datetime import datetime, timedelta

# setup databases & collections
appDb = mongo_client['app-db']
analyticsDb = mongo_client['analytics-db']
contents = appDb['contents']
userStats = analyticsDb['creatorStats']

def handle(event, context):
    print(json.dumps(event, indent=4))

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
            # summarize by content type
            '$group': {
                '_id': {
                    '_id': '$author.id', 
                    'type': '$type'
                }, 
                'contentCount': {
                    '$count': {}
                }, 
                'likedCount': {
                    '$sum': '$engagements.like.count'
                }, 
                'commentedCount': {
                    '$sum': '$engagements.comment.count'
                }, 
                'recastedCount': {
                    '$sum': '$engagements.recast.count'
                }, 
                'quotedCount': {
                    '$sum': '$engagements.quote.count'
                }
            }
        }, {
            # summarize by user (not account)
            '$group': {
                '_id': '$_id._id',
                'CreatorContentCount': {
                    '$sum': '$contentCount'
                }, 
                'CreatorLikedCount': {
                    '$sum': '$likeCount'
                }, 
                'CreatorCommentedCount': {
                    '$sum': '$commentCount'
                }, 
                'CreatorRecastedCount': {
                    '$sum': '$recastCount'
                }, 
                'CreatorQuotedCount': {
                    '$sum': '$quoteCount'
                }, 
                'contentSummary': {
                    '$push': {
                        'type': '$_id.type', 
                        'typeCount': '$contentCount', 
                        'likedCount': '$likeCount', 
                        'commentedCount': '$commentCount', 
                        'recastedCount': '$recastCount', 
                        'quotedCount': '$quoteCount'
                    }
                }
            }
        }, {
            # join w/ 'users' collections for more info.
            '$lookup': {
                'from': 'users', 
                'localField': '_id', 
                'foreignField': '_id', 
                'as': 'userDetail'
            }
        }, {
            # setting format
            '$project': {
                '_id': 1, 
                'ownerAccount': {
                    '$first': '$CreatorDetail.ownerAccount'
                }, 
                'displayId': {
                    '$first': '$userDetail.displayId'
                }, 
                'createdAt': {
                    '$first': '$userDetail.createdAt'
                }, 
                'visibility': {
                    '$first': '$userDetail.visibility'
                }, 
                'followedCount': {
                    '$first': '$userDetail.followedCount'
                }, 
                'followerCount': {
                    '$first': '$userDetail.followerCount'
                }, 
                'CreatorContentCount': 1, 
                'CreatorLikedCount': 1, 
                'CreatorCommentedCount': 1, 
                'CreatorRecastedCount': 1, 
                'CreatorQuotedCount': 1
            }
        }, {
            # upsert to 'userStats' collection
            '$merge': {
                'into': {
                    'db': 'analytics-db', 
                    'coll': 'creatorStats'
                }, 
                'on': '_id', 
                'whenMatched': 'replace', 
                'whenNotMatched': 'insert'
            }
        }
    ]

    try:
        # perform aggregation w/ resulting in upsert 'userStats' collection
        contents.aggregate(cursor)

        # print message on complete aggregation
        print('this aggregation has completed at', datetime.now())

    except Exception as error:
        print("ERROR", error)
