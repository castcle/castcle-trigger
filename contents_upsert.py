# This file updates collection 'creatorStats' from 'contents'
import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from datetime import datetime, timedelta
import math

# setup databases & collections
appDb = mongo_client['app-db']
analyticsDb = mongo_client['analytics-db']
contents = appDb['contents']
userStats = analyticsDb['creatorStats']

def handle(event, context):
    print(json.dumps(event, indent=4))

    # define content parameters
    contentDateThreshold = 14
    likedWeight = 1
    commentedWeight = 1
    recastedWeight = 1
    quotedWeight = 1
    followedWeight = 0.01
    halfLifeHours = 24

    # define cursor
    cursor = [
        {
            # filter age of contents for only newer than specific days
            # filter only visible contents
            '$match': {
                'createdAt': {
                    '$gte': (datetime.utcnow() - timedelta(days=contentDateThreshold)) 
                },
                'visibility': 'publish'
            }
        }, {
            # summarize to get summation of engagements for each content type & user
            '$group': {
                '_id': {
                    'authorId': '$author.id', 
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
                }, 
                'updatedAt': {
                    '$max': '$updatedAt'
                }
            }
        }, {
            # add calculated fields
            '$addFields': {
                # age score: a decay value as time since last update time
                ## equation: ageScore = e^(-{\lambda}*t)
                'aggregator.ageScore': {
                    '$exp': {
                        '$multiply': [
                            {
                                '$multiply': [
                                    {
                                        '$divide': [
                                            {
                                                '$subtract': [
                                                    datetime.utcnow(), '$updatedAt'
                                                ]
                                            }, 60 * 60 * 1000
                                        ]
                                    }, {
                                        '$divide': [
                                            {
                                                '$ln': 2
                                            }, 24
                                        ]
                                    }
                                ]
                            }, -1
                        ]
                    }
                }, 
                # engagement score: a linear combination result among type of available engagements
                ## equation: engagementScore = {\sigma}_{k}({\beta}_{k}*x_{k})
                'aggregator.engagementScore': {
                    '$sum': [
                            {
                                '$multiply': [
                                    '$commentedCount', commentedWeight
                                ]
                            }, {
                                '$multiply': [
                                    '$recastedCount', recastedWeight
                                ]
                            }, {
                                '$multiply': [
                                    '$quotedCount', quotedWeight
                                ]
                            }, {
                                '$multiply': [
                                    '$quotedCount', quotedWeight
                                ]
                            # add bias = 1
                            }, 1
                        ]
                }
            }
        }, {
            # summarize to get summation of engagements for each user
            '$group': {
                '_id': '$_id.authorId', 
                'creatorContentCount': {
                    '$sum': '$contentCount'
                }, 
                'creatorLikedCount': {
                    '$sum': '$likeCount'
                }, 
                'creatorCommentedCount': {
                    '$sum': '$commentCount'
                }, 
                'creatorRecastedCount': {
                    '$sum': '$recastCount'
                }, 
                'creatorQuotedCount': {
                    '$sum': '$quoteCount'
                }, 
                'updatedAt': {
                    '$max': '$updatedAt'
                }, 
                'summary': {
                    '$push': {
                        'type': '$_id.type', 
                        'typeCount': '$contentCount', 
                        'updatedAt': '$updatedAt', 
                        'aggregator': '$aggregator'
                    }
                }
            }
        }, {
            # deconstruct object, named "summary" for furthor calculation
            '$unwind': {
                'path': '$summary', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            # calculate total engagementScore
            '$addFields': {
                'aggregator.engagementScore': {
                    '$sum': [
                        {
                            '$multiply': [
                                '$creatorLikedCount', commentedWeight
                            ]
                        }, {
                            '$multiply': [
                                '$creatorLikedCount', recastedWeight
                            ]
                        }, {
                            '$multiply': [
                                '$creatorCommentedCount', quotedWeight
                            ]
                        }, {
                            '$multiply': [
                                '$creatorQuotedCount', quotedWeight
                            ]
                        # add bias = 1
                        }, 1
                    ]
                }, 
                # calculate weights as fractions of content type per total contents
                ## equation: typeWeight(type) = n_{content|type}/N_{content}
                'summary.typeWeight': {
                    '$divide': [
                        '$summary.typeCount', '$creatorContentCount'
                    ]
                }
            }
        }, {
            # join with 'users' for more information
            '$lookup': {
                'from': 'users', 
                'localField': '_id', 
                'foreignField': '_id', 
                'as': 'userDetail'
            }
        }, {
            # filter for only publish users
            '$match': {
                'userDetail.visibility': 'publish'
            }
        }, {
            # deconstruct 'userDetail' for accessibility
            '$unwind': {
                'path': '$userDetail', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            # calculate followed score then add to both aggregator
            ## equation: followedScore = (\gamma}*N_{follower}
            '$addFields': {
                'aggregator.followedScore ': {
                    '$multiply': [
                        '$userDetail.followedCount', followedWeight
                    ]
                }, 
                'summary.aggregator.followedScore': {
                    '$multiply': [
                        '$userDetail.followedCount', followedWeight
                    ]
                }
            }
        }, {
            # map intermediate result format
            '$project': {
                '_id': 1, 
                'updatedAt': 1, 
                'creatorContentCount': 1, 
                'summary.type': 1, 
                'summary.typeCount': 1, 
                'summary.updatedAt': 1, 
                'summary.typeWeight': 1, 
                'summary.aggregator': 1, 
                'aggregator.engagementScore': 1, 
                'aggregator.followedScore': 1, 
                'ownerAccount': '$userDetail.ownerAccount', 
                'displayId': '$userDetail.displayId', 
                'createdAt': '$userDetail.createdAt', 
                'followedCount': '$userDetail.followedCount', 
                'followerCount': '$userDetail.followerCount', 
                # calculate creator score for each content type
                ## equation: score = ((typeWeight)*(ageScore)*(engagementScore_{type}/engagementScore)) + followedScore
                'summary.score': {
                    '$add': [
                        {
                            '$multiply': [
                                '$summary.typeWeight', {
                                    '$divide': [
                                        {
                                            '$add': [
                                                '$summary.aggregator.engagementScore', 1
                                            ]
                                        }, {
                                            '$add': [
                                                '$aggregator.engagementScore', 1
                                            ]
                                        }
                                    ]
                                }, '$summary.aggregator.ageScore'
                            ]
                        }, '$summary.aggregator.followedScore'
                    ]
                }
            }
        }, {
            # undo the previous '$unwind'
            '$group': {
                '_id': '$_id', 
                'ownerAccount': {
                    '$max': '$ownerAccount'
                }, 
                'displayId': {
                    '$max': '$displayId'
                }, 
                'creatorContentCount': {
                    '$max': '$creatorContentCount'
                }, 
                'createdAt': {
                    '$max': '$createdAt'
                }, 
                'updatedAt': {
                    '$max': '$updatedAt'
                }, 
                'followedCount': {
                    '$max': '$followedCount'
                }, 
                'followerCount': {
                    '$max': '$followerCount'
                }, 
                'summary': {
                    '$push': '$summary'
                }, 
                'aggregator': {
                    '$max': '$aggregator'
                }, 
                # add the latest ageScore to overall aggregator
                'ageScore': {
                    '$max': '$summary.aggregator.ageScore'
                }
            }
        }, {
            # map final result format
            '$project': {
                '_id': 1, 
                'ownerAccount': 1, 
                'displayId': 1, 
                'createdAt': 1, 
                'updatedAt': 1, 
                'followedCount': 1, 
                'followerCount': 1, 
                'summary': 1, 
                'aggregator.ageScore': '$ageScore', 
                'aggregator.engagementScore': '$aggregator.engagementScore', 
                'aggregator.followedScore': '$aggregator.followedScore', 
                # calculate overall creator score
                ## equation: score = (ageScore*(engagementScore_{type}/engagementScore)) + (followedScore + 0.1)
                'score': {
                '$add': [
                    {
                        '$multiply': [
                            {
                                '$divide': [
                                    '$aggregator.engagementScore', '$creatorContentCount'
                                ]
                            }, '$ageScore'
                        ]
                    }, {
                        '$add': [
                            # add bias = 0.01
                            '$aggregator.followedScore', 0.01
                        ]
                    }
                ]
            }
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
        # perform aggregation w/ resulting in upsert 'creatorStats' collection
        contents.aggregate(cursor)

        # print message on complete aggregation
        print('this aggregation has completed at', datetime.utcnow())

    except Exception as error:
        print("ERROR", error)
