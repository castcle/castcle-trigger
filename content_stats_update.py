# This file is not work yet
# ultimate goal is to return contents/contentId w/ reasons as output for 'feedItem' collection
import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from bson import regex
from datetime import datetime, timedelta
import re

# setup databases & collections
appDb = mongo_client['app-db']
analyticsDb = mongo_client['analytics-db']
contents = appDb['contents']
creatorStats = analyticsDb['creatorStats']
hashtagStats = analyticsDb['hashtagStats']


def handle(event, context):
    print(json.dumps(event, indent=4))

# define cursor
# now we have 3 effective collections i.e. 'creatorStats', 'hashtagStat', 'contents'
# 1. filter only potential related contents as aggregator
# 2. ordering for obain top potential contents as ranker

    # define query parameters
    hashtagThreshold = 5
    hashtagDateThreshold = 7

    creatorThreshold = 100
    creatorDateThreshold = 7

    #################################################################
    # find top hashtag #
    #################################################################
    # define cursor
    hashtagCursor = [
        {
            # filter for hashtags those are updated within specific days
            '$match': {
                'updatedAt': {
                    '$gte': (datetime.now() - timedelta(days=hashtagDateThreshold))
                }
            }
        }, {
            # filter non-hashtag out to prevent bias
            '$match': {
                '_id': {
                    '$ne': ''
                }
            }
        }, {
            # order by number of occurance
            '$sort': {
                'hashtagCount': -1
            }
        }, {
            # slice for top numbers
            '$limit': hashtagThreshold
        }, {
            # deconstruct for grouping
            '$unwind': {
                'path': '$contributorsDetail'
            }
        }, {
            # summarize all documents into a single array
            '$group': {
                '_id': None,
                'contents': {
                    '$push': '$contributorsDetail.contents'
                }
            }
            # project deconstruct all contentId into a sigle document with label reason as topHashtags
        }, {
            '$project': {
                '_id': 'topHashtags',
                'contentIds': {
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
        }
    ]

    #################################################################
    # aggregate contents from found hashtags
    #################################################################
    # define cursor
    creatorCursor = [
        {
            # filter for new than specific days contents
            '$match': {
                'lastContentAt': {
                    '$gte': (datetime.now() - timedelta(days=creatorDateThreshold))
                }
            }
        }, {
            # filter for only available creator users
            '$match': {
                'visibility': 'publish'
            }
        }, {
            # map count values into fractions
            '$project': {
                '_id': 1,
                'likedRate': {
                    '$divide': [
                        '$creatorLikedCount', '$creatorContentCount'
                    ]
                },
                'commentedRate': {
                    '$divide': [
                        '$creatorCommentedCount', '$creatorContentCount'
                    ]
                },
                'recastedRate': {
                    '$divide': [
                        '$creatorRecastedCount', '$creatorContentCount'
                    ]
                },
                'quotedRate': {
                    '$divide': [
                        '$creatorQuotedCount', '$creatorContentCount'
                    ]
                },
                'followedCount': 1,
                'lastContentAt': 1,
                'contentSummary': 1
            }
        }, {
            # order by fractions
            '$sort': {
                'quotedRate': -1,
                'recastedRate': -1,
                'commentedRate': -1,
                'likedRate': -1,
                'lastContentAt': -1
            }
        }, {
            # slice for top creator creator users
            '$limit': creatorThreshold
        }, {
            # deconstruct for grouping
            '$unwind': {
                'path': '$contentSummary'
            }
        }, {
            # summarize all documents into a single array
            '$group': {
                '_id': None,
                'contents': {
                    '$push': '$contentSummary.contents'
                }
            }
        }, {
            # project deconstruct all contentId into a sigle document with label reason as topCreators
            '$project': {
                '_id': 'topCreators',
                'contentIds': {
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
        }
    ]

    try:
        # top hashtags
        # aggregate then keep results as array of contentIds
        topHashtagContents = list(hashtagStats.aggregate(hashtagCursor))[
            0]['contentIds']

        # print message on complete aggregation
        print('hashtagStats aggregator has returned',
              len(topHashtagContents), 'contents')
        print('hashtagStats aggregation has completed at', datetime.now())

        # top creators
        # aggregate then keep results as array of contentIds
        topCreatorContents = list(creatorStats.aggregate(creatorCursor))[
            0]['contentIds']

        # print message on complete aggregation
        print('creatorStats aggregator has returned',
              len(topCreatorContents), 'contents')
        print('creatorStats aggregation has completed at', datetime.now())

        # aggregatedPool
        # unique combine aggregated content IDs
        aggregatedPool = topHashtagContents + \
            list(set(topCreatorContents) - set(topHashtagContents))

        # print message on complete combining
        print('aggregatedPool has total', len(topCreatorContents), 'contents')

        # print message on complete implemtation
        print('this aggregation has completed at', datetime.now())

    except Exception as error:
        print("ERROR", error)

    #################################################################
