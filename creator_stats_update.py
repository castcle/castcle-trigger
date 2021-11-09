# This file updates collection 'creatorStats' from 'contents'
import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from datetime import datetime, timedelta
import math

# setup databases & collections
appDb = mongo_client['app-db']
contents = appDb['contents']


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        print("WarmUp - Lambda is warm!")
        return

    print(json.dumps(event, indent=4))

    # define content parameters
    contentDateThreshold = 14
    likedWeight = 1
    commentedWeight = 1
    recastedWeight = 1
    quotedWeight = 1
    followedWeight = 0.01
    halfLifeHours = 24

    try:

        # define cursor
        creatorStatsCursor = [
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
                    # equation: ageScore = e^(-{\lambda}*t)
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
                                                }, halfLifeHours
                                            ]
                                        }
                                    ]
                                }, -1
                            ]
                        }
                    },
                    # engagement score: a linear combination result among type of available engagements
                    # equation: engagementScore = {\sigma}_{k}({\beta}_{k}*x_{k})
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
                    # equation: typeWeight(type) = n_{content|type}/N_{content}
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
                # calculate followed score then add to both aggregator
                # equation: followedScore = (\gamma}*N_{follower}
                '$addFields': {
                    'followedScore': {
                        '$multiply': [
                            {
                                '$first': '$userDetail.followedCount'
                            }, followedWeight
                        ]
                    }
                }
            }, {
                # map intermediate result format
                '$project': {
                    '_id': 1,
                    'creatorContentCount': 1,
                    'summary.type': 1,
                    'summary.typeCount': 1,
                    'summary.typeWeight': 1,
                    'summary.aggregator.ageScore': 1,
                    'summary.aggregator.engagementScore': 1,
                    'aggregator.engagementScore': 1,
                    'aggregator.followedScore': '$followedScore',
                    'lastContentUpdatedAt': '$updatedAt',
                    'summary.lastContentUpdatedAt': '$updatedAt',
                    'summary.aggregator.followedScore': '$followedScore',
                    'ownerAccount': {
                        '$first': '$userDetail.ownerAccount'
                    },
                    'displayId': {
                        '$first': '$userDetail.displayId'
                    },
                    'createdAt': {
                        '$first': '$userDetail.createdAt'
                    },
                    'followedCount': {
                        '$first': '$userDetail.followedCount'
                    },
                    'followerCount': {
                        '$first': '$userDetail.followerCount'
                    },
                    'summary.aggregator.followedScore': '$followedScore',
                    # calculate creator score for each content type
                    # equation: score = ((typeWeight)*(ageScore)*(engagementScore_{type}/engagementScore)) + followedScore
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
                            }, '$followedScore'
                        ]
                    },
                    'creatorLikedCount': '1',
                    'creatorCommentedCount': '1',
                    'creatorRecastedCount': '1',
                    'creatorQuotedCount': '1'
                }
            }, {
                # summarize content type together
                '$group': {
                    '_id': '$_id',
                    'ownerAccount': {
                        '$max': '$ownerAccount'
                    },
                    'displayId': {
                        '$max': '$displayId'
                    },
                    'userCreatedAt': {
                        '$max': '$createdAt'
                    },
                    'lastContentUpdatedAt': {
                        '$max': '$lastContentUpdatedAt'
                    },
                    'creatorContentCount': {
                        '$max': '$creatorContentCount'
                    },
                    'followedCount': {
                        '$max': '$followedCount'
                    },
                    'followerCount': {
                        '$max': '$followerCount'
                    },
                    'aggregator': {
                        '$max': '$aggregator'
                    },
                    'summary': {
                        '$push': '$summary'
                    },
                    'ageScore': {
                        '$max': '$summary.aggregator.ageScore'
                    },
                    'creatorLikedCount': {
                        '$sum': '$creatorLikedCount'
                    },
                    'creatorCommentedCount': {
                        '$sum': '$creatorCommentedCount'
                    },
                    'creatorRecastedCount': {
                        '$sum': '$creatorRecastedCount'
                    },
                    'creatorQuotedCount': {
                        '$sum': '$creatorQuotedCount'
                    }
                }
            }, {
                # map final result format
                '$project': {
                    '_id': 1,
                    'ownerAccount': 1,
                    'displayId': 1,
                    'userCreatedAt': 1,
                    'lastContentUpdatedAt': 1,
                    'followedCount': 1,
                    'followerCount': 1,
                    'summary': 1,
                    'contentCount': '$creatorContentCount',
                    'aggregator.ageScore': '$ageScore',
                    'aggregator.engagementScore': '$aggregator.engagementScore',
                    'aggregator.followedScore': '$aggregator.followedScore',
                    # calculate overall creator score
                    # equation: score = (ageScore*(engagementScore_{type}/engagementScore)) + (followedScore + bias)
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
                                    '$aggregator.followedScore', followedWeight
                                ]
                            }
                        ]
                    },
                    'creatorLikedCount': '$creatorLikedCount',
                    'creatorCommentedCount': '$creatorCommentedCount',
                    'creatorRecastedCount': '$creatorRecastedCount',
                    'creatorQuotedCount': '$creatorQuotedCount'
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

        # perform aggregation w/ resulting in upsert 'creatorStats' collection
        contents.aggregate(creatorStatsCursor)

        # print message on complete aggregation
        print('this aggregation has completed at', datetime.utcnow())

    except Exception as error:
        print("ERROR", error)
