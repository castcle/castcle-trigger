# This file updates collection 'hashtagStats' from 'contents'
import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from bson import regex
from datetime import datetime, timedelta
import re
import math

# setup databases & collections
appDb = mongo_client['app-db']
analyticsDb = mongo_client['analytics-db']
contents = appDb['contents']
hashtagStats = analyticsDb['hashtagStats']


def handle(event, context):
    print(json.dumps(event, indent=4))

    # define RegEx
    pattern = regex.Regex.from_native(re.compile(r"(?<=#)\w+"))
    pattern.flags ^= re.UNICODE

    # define content parameters
    contentDateThreshold = 14
    likedWeight = 1
    commentedWeight = 1
    recastedWeight = 1
    quotedWeight = 1
    halfLifeHours = 24

    # define cursor
    cursor = [
        {
            # filter contents for newer than specific age
            '$match': {
                'createdAt': {
                    '$gte': (datetime.utcnow() - timedelta(days=contentDateThreshold)) 
                    }
            }
        }, {
            # extract hashtag => array
            '$addFields': {
                'hashtags': {
                    '$regexFindAll': {
                        'input': '$payload.message', 
                        'regex': pattern
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
            'name': {
                '$toLower': '$hashtags.match'
                }
            }
        }, {
            # summarize by user (not account)
            # collect contentId as array
            '$group': {
                '_id': {
                    'name': '$name',
                    'authorId': '$author.id'
                }, 
                'contributionCount': {
                    '$count': {}
                }, 
                'createdAt': {
                    '$min': '$createdAt'
                }, 
                'updatedAt': {
                    '$max': '$updatedAt'
                },
                'contents': {
                    '$push': "$_id"
                },
                # ! follow app-db.hashtags schema
                '__v': {
                    '$max': '$__v'
                },
                'likedCount': {
                    '$sum': '$engagements.like.count'
                },
                'commentedCount': {
                    '$sum': '$engagements.comment.count'
                },
                'recastedCount': {
                    '$sum': '$engagements.recast.recast'
                },
                'quotedCount': {
                    '$sum': '$engagements.quote.recast'
                }
            }
        }, {
            # summarize by hashtag
            '$group': {
                '_id': '$_id.name', 
                'hashtagCount': {
                    '$sum': '$contributionCount'
                }, 
                'contributorCount': {'$count': {}},
                'createdAt': {
                    '$min': '$createdAt'
                }, 
                'updatedAt': {
                    '$max': '$updatedAt'
                },
                # ! follow app-db.hashtags schema
                '__v': {
                    '$max': '$__v'
                },
                'contributions': {
                    '$push': {
                        '_id': '$_id.authorId', 
                        'contributionCount': '$contributionCount', 
                        'contents': "$contents",
                    }
                },
                'likedCount': {
                    '$sum': '$likedCount'
                },
                'commentedCount': {
                    '$sum': '$commentedCount'
                },
                'recastedCount': {
                    '$sum': '$recastedCount'
                },
                'quotedCount': {
                    '$sum': '$quotedCount'
                }
            }    
        }, {
            # setting output format
            '$project': {
                '_id': 0,  
                'name': '$_id',
                '__v': '$__v',
                'createdAt': 1, 
                'updatedAt': 1, 
                'aggregator.contributions': '$contributions',
                # calculate fraction of hashtag diversity
                'aggregator.hastagDiversityScore': {
                    '$divide': [
                        '$contributorCount', '$hashtagCount'
                    ]
                },
                # calculate linear combination of engagements 
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
                        }
                    ]
                },
                # calculate decay from last update time
                'aggregator.ageScore': {
                    '$exp': {
                        '$multiply': [
                            {
                                '$multiply': [
                                    {
                                        # calculate age from last update time
                                        '$divide': [
                                            {
                                                '$subtract': [datetime.utcnow(), "$updatedAt"]
                                            }, 60*60*1000
                                        ]
                                    }, {
                                        # define lambda value
                                        '$divide': [{'$ln': 2}, halfLifeHours]
                                    }
                                ]
                            }, -1
                        ]
                    }
                }            
            }
        }, {
            # summarize all scores
            '$addFields': {
                'score': {
                    '$multiply': [
                        '$aggregator.hastagDiversityScore',
                        # add 1 as bias
                        {'$add': ['$aggregator.engagementScore', 1]},
                        '$aggregator.ageScore'
                    ]
                }
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
        print('this aggregation has completed at', datetime.utcnow())

    except Exception as error:
        print("ERROR", error)
