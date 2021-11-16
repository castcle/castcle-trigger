import os
import pickle
import bson.objectid
from bson import ObjectId
from datetime import datetime, timedelta
import pandas as pd
import xgboost as xgb
from mongo_client import mongo_client

# define save model artifact to database function
def save_model_to_mongodb(dst_database_name: str,
                          dst_collection_name: str, 
                          model_name: str, 
                          account_id, #! using user id, in future version change to account id
                          model_artifact,
                          features_list):

    pickled_model = pickle.dumps(model_artifact) # pickling the model

    document = mongo_client[dst_database_name][dst_collection_name].update_one(
        {
            'account': account_id,
            'model': model_name,
        }, {
            '$set': {
                'account': account_id,
                'model': model_name,
                'artifact': pickled_model,
                'trainedAt': datetime.now(),
                'features' : features_list
            }
        }, upsert= True)

    return None

# define main function for training as follow personalize content
def personalized_content_trainer_main(updatedAtThreshold: float, # define content age
                                       app_db: str,
                                       engagement_collection: str ,
                                       analytics_db: str,
                                       creator_stats_collection: str,
                                       content_stats_collection: str,
                                       dst_database_name: str,
                                       dst_collection_name: str,
                                       model_name: str):
    
    # define cursor of content features
    contentFeaturesCursor = [
        {
            # filter age of contents for only newer than specific days
            # filter only visible contents
            '$match': {
                'updatedAt': {
                    '$gte': (datetime.utcnow() - timedelta(days=updatedAtThreshold))
                }
            }
        }, {
            # join with creator stats
            '$lookup': {
                'from': creator_stats_collection, # previous:'creatorStats',
                'localField': 'authorId',
                'foreignField': '_id',
                'as': 'creatorStats'
            }
        }, {
            # deconstruct array
            '$unwind': {
                'path': '$creatorStats',
                'preserveNullAndEmptyArrays': True
            }
        }, {
            # map output format
            '$project': {
                '_id': 1,
                'likeCount': 1,
                'commentCount': 1,
                'recastCount': 1,
                'quoteCount': 1,
                'photoCount': 1,
                'characterLength': 1,
                'creatorContentCount' :'$creatorStats.contentCount',
                'creatorLikedCount': '$creatorStats.creatorLikedCount',
                'creatorCommentedCount': '$creatorStats.creatorCommentedCount',
                'creatorRecastedCount': '$creatorStats.creatorRecastedCount',
                'creatorQuotedCount': '$creatorStats.creatorQuotedCount',
                'ageScore': '$aggregator.ageScore'
#                 # alias 'total label'
#                 'engagements': {
#                     '$sum': [
#                         '$likeCount', 
#                         '$commentCount',
#                         '$recastCount',
#                         '$quoteCount'
#                     ]
#                 }
            }
        }
    ]

    # assign result to dataframe
    # alias 'contentFeatures_1'
    content_features = pd.DataFrame(list(mongo_client[analytics_db][content_stats_collection].aggregate(contentFeaturesCursor))).rename({'_id':'contentId'},axis = 1)
    
#     #! only in testing
#     pprint(list(mongo_client[analytics_db][content_stats_collection].aggregate(contentFeaturesCursor)))

    # define cursor of engagement transaction
    transactionEngagementsCursor = [
        {
            # summarize by pairing of user ID & content ID
            '$group': {
                '_id': {
                    'userId': '$user',
                    'contentId': '$targetRef.$id'
                },
                'engangements': {
                    '$push': '$type'
                }
            }
        }, {
            # deconstruct for ease of adding fields
            '$unwind': {
                'path': '$engangements'
            }
        }, {
            # add fields by matching engagement types
            '$addFields': {
                'like': {
                    '$toInt': {
                        '$eq': [
                            '$engangements', 'like'
                        ]
                    }
                },
                'comment': {
                    '$toInt': {
                        '$eq': [
                            '$engangements', 'comment'
                        ]
                    }
                },
                'recast': {
                    '$toInt': {
                        '$eq': [
                            '$engangements', 'recast'
                        ]
                    }
                },
                'quote': {
                    '$toInt': {
                        '$eq': [
                            '$engangements', 'quote'
                        ]
                    }
                }
            }
        }, {
            # summarize to merge all added engagement types
            '$group': {
                '_id': '$_id',
                'like': {
                    '$first': '$like'
                },
                'comment': {
                    '$first': '$comment'
                },
                'recast': {
                    '$first': '$recast'
                },
                'quote': {
                    '$first': '$quote'
                }
            }
        }, {
            # map output format as followed requirement
            '$project': {
                '_id': 0,
                'userId': '$_id.userId',
                'contentId': '$_id.contentId',
                'like': '$like',
                'comment': '$comment',
                'recast': '$recast',
                'quote': '$quote',
                # alias 'label'
                'engagements': {
                    '$sum': [
                        '$like', 
                        '$comment',
                        '$recast',
                        '$quote'
                    ]
                }
            }
        }
    ]

    # assign result to dataframe
    transaction_engagements = pd.DataFrame(list(mongo_client[app_db][engagement_collection].aggregate(transactionEngagementsCursor)))
    
#     # only in testing
#     pprint(list(mongo_client[app_db][engagement_collection].aggregate(transactionEngagementsCursor)))
    
    # join together
    transaction_engagements = transaction_engagements.merge(content_features,
                                                        on='contentId',
                                                        how='left')
    
    ## in case of 'creatorStats' does not update yet -> fill NaN
    transaction_engagements.fillna(0,inplace=True)

#     ## simply explore dataframe
#     print(transaction_engagements.head(2))
#     print('\n')

    select_user = transaction_engagements.groupby('userId')['contentId'].agg('count').reset_index()
    
    # select only user with ever engaged more than 2 contents 
    select_user = select_user[select_user['contentId'] > 2]
    
    
    ## model training
    ml_artifacts = [] # pre-define model artifacts

    # loop through user id of selected user
    for user in list(select_user.userId.unique()):
        
        # filter for only selected user
        focused_transaction_engagements = transaction_engagements[transaction_engagements['userId'] == user]  
        
        # summary engagements of selected user
        portion = focused_transaction_engagements.groupby('userId').agg( 
                                like_count = ('like','sum'),
                                comment_count = ('comment','sum'),
                                recast_count = ('recast','sum'),
                                quote_count = ('quote','sum')
                                ).reset_index().replace(0,1)

        # formalize features
        portion = portion[['like_count','comment_count','recast_count','quote_count']].div(portion.sum(axis=1)[0]).div(-1)+1
        focused_transaction_engagements.loc[:,'like'] = focused_transaction_engagements.loc[:,'like'] * portion.loc[0,'like_count']
        focused_transaction_engagements.loc[:,'comment'] = focused_transaction_engagements.loc[:,'comment'] * portion.loc[0,'comment_count']
        focused_transaction_engagements.loc[:,'recast'] = focused_transaction_engagements.loc[:,'recast'] * portion.loc[0,'recast_count']
        focused_transaction_engagements.loc[:,'quote'] = focused_transaction_engagements.loc[:,'quote'] * portion.loc[0,'quote_count']
        focused_transaction_engagements['engagements'] = focused_transaction_engagements['like'] + focused_transaction_engagements['comment'] + focused_transaction_engagements['recast'] + focused_transaction_engagements['quote']  

        # separate features & label
        features = focused_transaction_engagements.drop(['engagements','userId','contentId','like','comment','recast','quote'],axis = 1)
        label = focused_transaction_engagements['engagements']
        
#         #! only testing
#         print(list(features.columns))
    
        # define estimator
        xg_reg = xgb.XGBRegressor(random_state = 123)
        
        # fitting model
        xg_reg.fit(features, label)
        
        # print result
        print('finish training user id:')
        print(user)
        
        ml_artifacts.append(xg_reg) # collect list of artifacts

        # upsert to database
        save_model_to_mongodb(dst_database_name = dst_database_name,
                              dst_collection_name = dst_collection_name,
                              model_name= model_name,
                              account_id = user,   
                              model_artifact = xg_reg,
                              features_list = list(features.columns))
    return None