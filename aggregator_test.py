# This file aims to update model artifact

import json
import sys
from mongo_client import mongo_client
from bson.objectid import ObjectId
from bson import regex
from datetime import datetime, timedelta
import re
import math
import pickle
import pandas as pd
import xgboost as xgb

# assign databases
appDb = mongo_client['app-db']
analyticsDb = mongo_client['analytics-db']

# assign collections
# source collections
engagements = appDb['engagements']

# destination collections
creatorStats = analyticsDb['creatorStats']
contentStats = analyticsDb['contentStats']
mlArtifacts = analyticsDb['mlArtifacts']

# mocked collection
mlArtifacts_mocked = analyticsDb['mlArtifacts_mocked']

# define upsert function


def save_model_to_mongodb(collection, model_name, account, model):
    pickled_model = pickle.dumps(model)  # pickling the model

    document = collection.update_one(
        {
            'account': account,
            'model': str(model_name),
        }, {
            '$set': {
                'account': account,
                'model': str(model_name),
                'artifact': pickled_model,
                'trainedAt': datetime.now()
            }
        }, upsert=True)


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        print("WarmUp - Lambda is warm!")
        return

    print(json.dumps(event, indent=4))

    # define content parameters
    updatedAtThreshold = 14

    try:

        # #################################################################
        # #################################################################
        # # define cursor of content features
        # contentFeaturesCursor = [
        #     {
        #         # filter age of contents for only newer than specific days
        #         # filter only visible contents
        #         '$match': {
        #             'updatedAt': {
        #                 '$gte': (datetime.utcnow() - timedelta(days=updatedAtThreshold))
        #             }
        #         }
        #     }, {
        #         # join with creator stats
        #         '$lookup': {
        #             'from': 'creatorStats',
        #             'localField': 'authorId',
        #             'foreignField': '_id',
        #             'as': 'userStats'
        #         }
        #     }, {
        #         # deconstruct array
        #         '$unwind': {
        #             'path': '$userStats',
        #             'preserveNullAndEmptyArrays': True
        #         }
        #     }, {
        #         # map output format
        #         '$project': {
        #             '_id': 1,
        #             'likeCount': 1,
        #             'commentCount': 1,
        #             'recastCount': 1,
        #             'quoteCount': 1,
        #             'photoCount': 1,
        #             'characterLength': 1,
        #             'creatorContentCount' :'$userStats.contentCount',
        #             'creatorLikedCount': '$userStats.creatorLikedCount',
        #             'creatorCommentedCount': '$userStats.creatorCommentedCount',
        #             'creatorRecastedCount': '$userStats.creatorRecastedCount',
        #             'creatorQuotedCount': '$userStats.creatorQuotedCount',
        #             'ageScore': '$aggregator.ageScore'
        #         }
        #     }
        # ]

        # # assign result to dataframe
        # content_features = pd.DataFrame(list(contentStats.aggregate(contentFeaturesCursor))).rename({'_id':'contentId'},axis = 1)

        # # define cursor of engagement transaction
        # transactionEngagementsCursor = [
        #     {
        #         # summarize by pairing of user ID & content ID
        #         '$group': {
        #             '_id': {
        #                 'userId': '$user',
        #                 'contentId': '$itemId'
        #             },
        #             'engangements': {
        #                 '$push': '$type'
        #             }
        #         }
        #     }, {
        #         # deconstruct for ease of adding fields
        #         '$unwind': {
        #             'path': '$engangements'
        #         }
        #     }, {
        #         # add fields by matching engagement types
        #         '$addFields': {
        #             'like': {
        #                 '$toInt': {
        #                     '$eq': [
        #                         '$engangements', 'like'
        #                     ]
        #                 }
        #             },
        #             'comment': {
        #                 '$toInt': {
        #                     '$eq': [
        #                         '$engangements', 'comment'
        #                     ]
        #                 }
        #             },
        #             'recast': {
        #                 '$toInt': {
        #                     '$eq': [
        #                         '$engangements', 'recast'
        #                     ]
        #                 }
        #             },
        #             'quote': {
        #                 '$toInt': {
        #                     '$eq': [
        #                         '$engangements', 'quote'
        #                     ]
        #                 }
        #             }
        #         }
        #     }, {
        #         # summarize to merge all added engagement types
        #         '$group': {
        #             '_id': '$_id',
        #             'like': {
        #                 '$first': '$like'
        #             },
        #             'comment': {
        #                 '$first': '$comment'
        #             },
        #             'recast': {
        #                 '$first': '$recast'
        #             },
        #             'quote': {
        #                 '$first': '$quote'
        #             }
        #         }
        #     }, {
        #         # map output format as followed requirement
        #         '$project': {
        #             '_id': 0,
        #             'userId': '$_id.userId',
        #             'contentId': '$_id.contentId',
        #             'like': '$like',
        #             'comment': '$comment',
        #             'recast': '$recast',
        #             'quote': '$quote'
        #         }
        #     }
        # ]

        # # assign result to dataframe
        # transaction_engagements = pd.DataFrame(list(engagements.aggregate(transactionEngagementsCursor)))

        # #################################################################
        # ## fill NaN just for testing
        # transaction_engagements.fillna(0,inplace=True)

        # ## simply explore dataframe
        # print(transaction_engagements.head(2))
        # print('\n')

        # #################################################################
        # select_user = transaction_engagements.groupby('userId')['contentId'].agg('count').reset_index()

        # select_user = select_user[select_user['contentId'] > 2]

        #################################################################
        #################################################################
        #################################################################
        # test on mocked data

        content_features = pd.DataFrame(
            list(analyticsDb['contentFeatures'].find()))
        content_features = content_features.fillna(0).rename(
            {'_id': 'contentId'}, axis=1).drop('userId', axis=1)

        transaction_engagements = pd.DataFrame(
            list(analyticsDb['transactionEngagements'].find()))

        transaction_engagements = transaction_engagements.merge(content_features,
                                                                on='contentId',
                                                                how='left')

        transaction_engagements['label'] = transaction_engagements['like'] + \
            transaction_engagements['comment'] + \
            transaction_engagements['recast'] + \
            transaction_engagements['quote']

        transaction_engagements.drop(['_id'], axis=1, inplace=True)

        # fill NaN just for testing
        transaction_engagements.fillna(0, inplace=True)

        select_user = transaction_engagements.groupby(
            'userId')['contentId'].agg('count').reset_index()

        select_user = select_user[select_user['contentId'] > 2]

        #################################################################
        #################################################################
        #################################################################

        ml_artifacts = []  # pre-define model artifacts

        for n in list(select_user.userId.unique()):

            focus_transaction = transaction_engagements[transaction_engagements['userId'] == n]
            portion = focus_transaction.groupby('userId').agg(
                like_count=('like', 'sum'),
                comment_count=('comment', 'sum'),
                recast_count=('recast', 'sum'),
                quote_count=('quote', 'sum')
            ).reset_index().replace(0, 1)

            portion = portion[['like_count', 'comment_count', 'recast_count', 'quote_count']].div(
                portion.sum(axis=1)[0]).div(-1) + 1
            focus_transaction.loc[:, 'like'] = focus_transaction.loc[:,
                                                                     'like'] * portion.loc[0, 'like_count']
            focus_transaction.loc[:, 'comment'] = focus_transaction.loc[:,
                                                                        'comment'] * portion.loc[0, 'comment_count']
            focus_transaction.loc[:, 'recast'] = focus_transaction.loc[:,
                                                                       'recast'] * portion.loc[0, 'recast_count']
            focus_transaction.loc[:, 'quote'] = focus_transaction.loc[:,
                                                                      'quote'] * portion.loc[0, 'quote_count']
            focus_transaction['label'] = focus_transaction['like'] + \
                focus_transaction['comment'] + \
                focus_transaction['recast'] + focus_transaction['quote']

            # simply print dataset
            print(focus_transaction.head(2))
            print('\n')

            Xlr = focus_transaction.drop(
                ['label', 'userId', 'contentId', 'like', 'comment', 'recast', 'quote'], axis=1)
            ylr = focus_transaction.label

            xg_reg = xgb.XGBRegressor()

            # simply print model on successful construction
            print(xg_reg)
            print('\n')

            xg_reg.fit(Xlr, ylr)
            #! debug
            print(Xlr)
            print("debug columns")
            print(list(Xlr.columns))
            # simply print considering user
            print(n)
            print('\n')

            ml_artifacts.append(xg_reg)  # collect list of artifacts

            # simply print to diagnosis
            print(ml_artifacts)
            print('\n')

            # upsert
            # !!! change collection back to  mlArtifacts when deploy
            save_model_to_mongodb(collection=mlArtifacts_mocked,
                                  account=n,
                                  model_name='xgboost',
                                  model=xg_reg)

        #################################################################
        #################################################################
        # print message on complete aggregation
        print('model artifacts has been updated')
        print('updating has completed at', datetime.utcnow())

    except Exception as error:
        print("ERROR", error)
