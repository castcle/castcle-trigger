'''
main function of update content statistics
1. check database
2. clear old contents
3. extract data from content in aspect of contents themselves then upserts to database
'''
import os
import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from bson import regex
from datetime import datetime, timedelta

# define function to remove old contents from 'contentStats' 
def remove_old_contents(contentDateThreshold: float,
                        database_name: str,
                        collection_name: str):

    '''
    remove/delete contents those have been updated older than "contentDateThreshold"
    '''
    
    # query statement to find contents with 'updatedAt' older than (less than) 'contentDateThreshold'
    query_statement = {
        'updatedAt': {
            '$lte': (datetime.utcnow() - timedelta(days=contentDateThreshold))
        }
    }
    
    #! remove contents as above query statement
    mongo_client[database_name][collection_name].remove(query_statement)
    
    return None

def update_content_stats_main(src_database_name: str,
                              src_collection_name: str,
                              dst_database_name: str,
                              dst_collection_name: str,
                              contentDateThreshold: float,
                              likedWeight: float,
                              commentedWeight: float,
                              recastedWeight: float,
                              quotedWeight: float,
                              halfLifeHours: float,
                              bias: float):

    '''
    main function of update content statistics
    1. check database
    2. clear old contents
    3. extract data from content in aspect of contents themselves then upserts to database
    '''

    try:

        # 1. check database
        if len(list(mongo_client[src_database_name][src_collection_name].find())) == 0:

            print('there is no document in', src_database_name, src_collection_name)

        else:

            # 2. clear old contents
            # perform content removal according to 'updatedAt'
            remove_old_contents(contentDateThreshold = contentDateThreshold,
                                database_name = dst_database_name, # query from dst
                                collection_name = dst_collection_name) # query from dst

            # print log
            print('contents age greater than', datetime.utcnow() - timedelta(days=contentDateThreshold), 'have been removed')

            # 3. extract data from content in aspect of contents themselves then upserts to database
            # define cursor
            contentStatsCursor = [
                {
                    # filter for only visible contents
                    '$match': {
                        'createdAt': {
                            '$gte': (datetime.utcnow() - timedelta(days=contentDateThreshold))
                        },
                        # consider 'visibility' is 'publish'
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
                                        '$engagements.like.count', likedWeight
                                    ]
                                }, {
                                    '$multiply': [
                                        '$engagements.comment.count', commentedWeight
                                    ]
                                }, {
                                    '$multiply': [
                                        '$engagements.recast.count', recastedWeight
                                    ]
                                }, {
                                    '$multiply': [
                                        '$engagements.quote.count', quotedWeight
                                    ]
                                }
                            ]
                        },
                        # project for investigation
                        # add photo count & message character length
                        '_id': 1,
                        'contentId': '$_id',
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
                                        '$aggregator.engagementScore', bias
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
                        'on': '_id', # cannot use 'contentId'
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

    return None