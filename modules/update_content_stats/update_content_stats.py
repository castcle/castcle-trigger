import os
import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from bson import regex
from datetime import datetime, timedelta
# import math

def update_content_stats_main(src_database_name: str,
                              src_collection_name: str,
                              dst_database_name: str,
                              dst_collection_name: str,
                              contentDateThreshold: float,
                              halfLifeHours: float):

    try:

        # define cursor
        contentStatsCursor = [
            {
                # filter for only visible contents
                '$match': {
                    'createdAt': {
                        '$gte': (datetime.utcnow() - timedelta(days=contentDateThreshold))
                    },
                    'visibility': 'publish'
                }
            }, {
                # map to calculate content low-level score
                '$project': {
                    # equation: ageScore = e^(-{\lambda}*t)
                    'aggregator.ageScore': {
                        '$exp': {
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
                                }, -1
                            ]
                        }
                    },
                    # equation: engagementScore = {\sigma}_{k}({\beta}_{k}*x_{k})
                    'aggregator.engagementScore': {
                        '$sum': [
                            {
                                '$multiply': [
                                    '$engagements.like.count', 1
                                ]
                            }, {
                                '$multiply': [
                                    '$engagements.comment.count', 1
                                ]
                            }, {
                                '$multiply': [
                                    '$engagements.recast.count', 1
                                ]
                            }, {
                                '$multiply': [
                                    '$engagements.quote.count', 1
                                ]
                            }
                        ]
                    },
                    # project for investigation
                    # add photo count & message character length
                    'updatedAt': 1,
                    'likeCount': '$engagements.like.count',
                    'commentCount': '$engagements.comment.count',
                    'recastCount': '$engagements.recast.count',
                    'quoteCount': '$engagements.quote.count',
                    'authorId': '$author.id',
                    'photoCount': {
                        '$size': {
                            '$ifNull': [
                                '$payload.photo.contents', []
                            ]
                        }
                    },
                    'characterLength': {
                        '$strLenCP': {
                            '$ifNull': [
                                '$payload.message', '-'
                            ]
                        }
                    }
                }
            }, {
                # scoring
                '$addFields': {
                    'score': {
                        '$multiply': [
                            {
                                '$add': [
                                    # add bias = 1
                                    '$aggregator.engagementScore', 1
                                ]
                            }, '$aggregator.ageScore'
                        ]
                    }
                }
            }, {
                # upsert to 'contentStats' collection
                ## equation: score = ageScore*(engagementScore + 1)*(hastagDiversityScore)
                '$merge': {
                    'into': {
                        'db': dst_database_name,
                        'coll': dst_collection_name
                    },
                    'on': '_id',
                    'whenMatched': 'replace',
                    'whenNotMatched': 'insert'
                }
            }
        ]

        # perform aggregation w/ resulting in upsert 'contentStats' collection
        mongo_client[src_database_name][src_collection_name].aggregate(contentStatsCursor)

        # print message on complete aggregation
        print('this aggregation has completed at', datetime.utcnow())

    except Exception as error:
        print("ERROR", error)