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
            # summarize by content type
            '$group': {
                '_id': {
                    '_id': '$author.id', 
                    'hashtag': '$hashtags.payload', 
                    'type': '$type'
                }, 
                'contentCount': {
                    '$count': {}
                }, 
                'likeCount': {
                    '$sum': '$engagements.like.count'
                }, 
                'commentCount': {
                    '$sum': '$engagements.comment.count'
                }, 
                'recastCount': {
                    '$sum': '$engagements.recast.count'
                }, 
                'quoteCount': {
                    '$sum': '$engagements.quote.count'
                }
            }
        }, {
            # summarize by hastag payload (not id)
            '$group': {
                '_id': {
                    '_id': '$_id._id', 
                    'hashtag': '$_id.hashtag'
                }, 
                'hashtagContentCount': {
                    '$sum': '$contentCount'
                }, 
                'hashtagLikeCount': {
                    '$sum': '$likeCount'
                }, 
                'hashtagCommentCount': {
                    '$sum': '$commentCount'
                }, 
                'hashtagRecastCount': {
                    '$sum': '$recastCount'
                }, 
                'hashtagQuoteCount': {
                    '$sum': '$quoteCount'
                }, 
                'contentSummary': {
                    '$push': {
                        'type': '$_id.type', 
                        'typeCount': '$contentCount', 
                        'likeCount': '$likeCount', 
                        'commentCount': '$commentCount', 
                        'recastCount': '$recastCount', 
                        'quoteCount': '$quoteCount'
                    }
                }
            }
        }, {
            # summarize by user (not account)
            '$group': {
                '_id': '$_id._id', 
                'userContentCount': {
                    '$sum': '$hashtagContentCount'
                }, 
                'userLikeCount': {
                    '$sum': '$hashtagLikeCount'
                }, 
                'userCommentCount': {
                    '$sum': '$hashtagCommentCount'
                }, 
                'userRecastCount': {
                    '$sum': '$hashtagRecastCount'
                }, 
                'userQuoteCount': {
                    '$sum': '$hashtagQuoteCount'
                }, 
                'hastagSummary': {
                    '$push': {
                        'hashtag': '$_id.hashtag', 
                        'hashtagContentCount': '$hashtagContentCount', 
                        'hashtagLikeCount': '$hashtagLikeCount', 
                        'hashtagCommentCount': '$hashtagCommentCount', 
                        'hashtagRecastCount': '$hashtagRecastCount', 
                        'hashtagQuoteCount': '$hashtagQuoteCount', 
                        'contentSummary': '$contentSummary'
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
                    '$first': '$userDetail.ownerAccount'
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
                'userContentCount': 1, 
                'userLikeCount': 1, 
                'userCommentCount': 1, 
                'userRecastCount': 1, 
                'userQuoteCount': 1, 
                'hastagSummary': 1
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

    except ERROR as error:
        print("ERROR", error)
