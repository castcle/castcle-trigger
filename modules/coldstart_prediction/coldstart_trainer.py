# main function of cold start model trainer
# 1. feature preparation
# 2. model training
# 3. model saveing
from pandas import pd

def extract_data(
        mongo_client, saved_model = 'mlArtifacts_country') -> pd.DataFrame:
    """
    Extract data from source, return only one data object
    """
    # connnect to database
    appDb = mongo_client['app-db']
    analyticsDb = mongo_client['analytics-db']
    
    mlArtifacts_country = analyticsDb[saved_model]

    def aggregate_features_country(
            mongo_client,
            updatedAtThreshold: float, app_db: str='app-db',
            engagement_collection: str='engagements'): 
        """
        Query and then prepare features from engagement collection
        """
    
        transactionEngagementsCountry = [
            {
            # filter age of contents for only newer than specific days
                '$match': {
                    'updatedAt': {
                        '$gte': (datetime.utcnow() - timedelta(days=updatedAtThreshold))
                    }
                }
            }, {
            # join with 'app-db.users' for account id
                '$lookup': {
                    'from': 'users', 
                    'localField': 'user', 
                    'foreignField': '_id', 
                    'as': 'users'
                }
            }, {
            # deconstruct array => object format
                '$unwind': {
                    'path': '$users'
                }
            }, {
            # join with 'app-db.accounts' of country code
                '$lookup': {
                    'from': 'accounts', 
                    'localField': 'users.ownerAccount', 
                    'foreignField': '_id', 
                    'as': 'accounts'
                }
            }, {
            # deconstruct array => object format
                '$unwind': {
                    'path': '$accounts'
                }
            }, {
            # group by content id & country code
                '$group': {
                    '_id': {
                        'contentId': '$targetRef.$id', 
                        'countryCode': '$accounts.geolocation.countryCode'
                    }, 
                    'engangements': {
                        '$push': '$type'
                    }
                }
            }, {
            # deconstruct array => object format
                '$unwind': {
                    'path': '$engangements'
                }
            }, {
            # convert engagement to integer type
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
            # group by again but sum engagement
                '$group': {
                    '_id': '$_id', 
                    'like': {
                        '$sum': '$like'
                    }, 
                    'comment': {
                        '$sum': '$comment'
                    }, 
                    'recast': {
                        '$sum': '$recast'
                    }, 
                    'quote': {
                        '$sum': '$quote'
                    }
                }
            }, {
            # map output format
                '$project': {
                    '_id': 0, 
                    'contentId': '$_id.contentId', 
                    'countryCode': '$_id.countryCode', 
                    'like': 1, 
                    'comment': 1, 
                    'recast': 1, 
                    'quote': 1
                }
            }
        ]
        # assign result to dataframe
        transaction_engagements_country_df = pd.DataFrame(list(
            mongo_client[app_db][engagement_collection]\
                .aggregate(transactionEngagementsCountry))
            )
    
        return transaction_engagements_country_df

    def aggregate_content_features(client, 
                        analytics_db: str,
                        content_stats_collection: str='contentStats',
                        creator_stats_collection: str='creatorStats') -> pd.DataFrame:


        #feature preparation using both "contentStats" & "creatorStats" 
        # then summary engagement behavior for each user

        # define cursor of content features
        contentFeaturesCursor = [
            {
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

                }
            }
        ]

        # assign result to dataframe
        # alias 'contentFeatures'

        content_features_df = pd.DataFrame(list(
            client[analytics_db][content_stats_collection]\
            .aggregate(contentFeaturesCursor)))\
                .rename({'_id':'contentId'},axis = 1)

        return content_features_df


    transaction_engagements_country = aggregate_features_country(
        updatedAtThreshold = updatedAtThreshold, # number of days to keep content
        app_db = 'app-db',
        engagement_collection = 'engagements')
    transaction_engagements_country['countryCode'] = \
        transaction_engagements_country['countryCode'].str.lower()

    contentFeatures_agg_df = aggregate_content_features(client = client, # default
                                        analytics_db = 'analytics-db',
                                        content_stats_collection = 'contentStats',
                                        creator_stats_collection = 'creatorStats')
    contentFeatures_clean_df = contentFeatures_agg_df.fillna(0)
    transaction_engagements_with_contentFeatures_df = transaction_engagements_country\
        .merge(contentFeatures_clean_df, on = 'contentId',how ='left')

    return transaction_engagements_with_contentFeatures_df

def transform_data(
    transaction_engagements_with_contentFeatures_df: pd.DataFrame) \
        -> pd.DataFrame:

    transaction_engagements_with_contentFeatures_df['label'] = (transaction_engagements_with_contentFeatures_df['like']
                                                            +transaction_engagements_with_contentFeatures_df['comment']
                                                            +transaction_engagements_with_contentFeatures_df['recast']
                                                            +transaction_engagements_with_contentFeatures_df['quote'])  

    # select country that can be trianed
    aggregated_transaction_engagements_df = transaction_engagements_with_contentFeatures_df.groupby('countryCode')['contentId'].agg('count').reset_index()
    selected_country_df = aggregated_transaction_engagements_df[aggregated_transaction_engagements_df['contentId'] > 2]

    return transaction_engagements_with_contentFeatures_df, selected_country_df

def modeling_and_saving(
    transaction_engagements_with_contentFeatures_df: pd.DataFrame, 
    selected_country_df: pd.DataFrame,
    mongo_client, 
    model_name='xgboost',
    based_model='th',
    updatedAtThreshold=30.0,
    saved_model = 'mlArtifacts_country') -> pd.DataFrame:
    """
    Modeling by user and then saving model binary into mongodb
    """
    import xgboost as xgb
    import pickle
    from datetime import datetime
    from pprint import pprint
    import iso3166 
    from datetime import datetime, timedelta

    analyticsDb = mongo_client['analytics-db']
    mlArtifacts_country = analyticsDb[saved_model]

    def save_model_to_mongodb(collection, model_name, account, model) -> None:

        pickled_model = pickle.dumps(model) # pickling the model

        document = collection.update_one(
            {
            'account': account,
            'model': str(model_name),
            }, {
            '$set': {
                'account': account,
                'model': str(model_name),
                'artifact': pickled_model,
                'trainedAt': datetime.utcnow(),
                'features' : list(features.columns)
            }
            }, upsert= True)

        print('[INFO] Coldstart model saved successfully')

        return None

    ml_artifacts = [] # pre-define model artifacts
    
    # loop through trainable country of selected country
    for n in list(selected_country_df.countryCode.unique()):
        
        # filter for only selected country
        selected_transaction_engagements_df = transaction_engagements_with_contentFeatures_df[transaction_engagements_with_contentFeatures_df['countryCode'] == n]  
        
        # summary engagements of selected country
        country_weight = selected_transaction_engagements_df.groupby('countryCode').agg( 
                                like_count = ('like','sum'),
                                comment_count = ('comment','sum'),
                                recast_count = ('recast','sum'),
                                quote_count = ('quote','sum')
                                                ).reset_index().replace(0,1)
        
         # formalize label
        country_weight = country_weight[['like_count','comment_count','recast_count','quote_count']].div(country_weight.sum(axis=1)[0]).div(-1)+1
        selected_transaction_engagements_df.loc[:,'like'] = selected_transaction_engagements_df.loc[:,'like']*country_weight.loc[0,'like_count']
        selected_transaction_engagements_df.loc[:,'comment'] = selected_transaction_engagements_df.loc[:,'comment']*country_weight.loc[0,'comment_count']
        selected_transaction_engagements_df.loc[:,'recast'] = selected_transaction_engagements_df.loc[:,'recast']*country_weight.loc[0,'recast_count']
        selected_transaction_engagements_df.loc[:,'quote'] = selected_transaction_engagements_df.loc[:,'quote']*country_weight.loc[0,'quote_count']
        selected_transaction_engagements_df['label'] = (selected_transaction_engagements_df['like']
                                                     +selected_transaction_engagements_df['comment']
                                                     +selected_transaction_engagements_df['recast']
                                                     +selected_transaction_engagements_df['quote'])

        # separate features & label
        features = selected_transaction_engagements_df.drop(['label','countryCode','contentId','like','comment','recast','quote'],axis = 1)
        label  = selected_transaction_engagements_df.label

        # define estimator
        xgboost_model = xgb.XGBRegressor(random_state = 123)
        
        # Fit to the model 
        xgboost_model.fit(features, label)
    
        pprint(n)
        
        # append result
        ml_artifacts.append(xgboost_model) # collect list of artifacts
    
        # upsert 
        save_model_to_mongodb(collection=mlArtifacts_country,
                          account=n,
                          model_name = model_name,
                          model=xgboost_model)

    # add Base model
    mlArtifacts = pd.DataFrame(list(mlArtifacts_country.find()))
    mlArtifacts_base_model = mlArtifacts[mlArtifacts['account'] == based_model].drop(['account'],axis = 1)
    
    # get country list
    country_list = list(set([x.lower() for x in iso3166.countries_by_alpha2.keys()]).difference(set(selected_country_df['countryCode'])))
    
    # loop for all country
    for i in country_list:
    # update collection  
        pprint(i)
        mlArtifacts_country.update_one(
               {
                'account': i,
                'model': mlArtifacts_base_model.iloc[0,1],
               }, {
                '$set': {
                    'account': i,
                    'model': str(mlArtifacts_base_model.iloc[0,1]),
                    'artifact': mlArtifacts_base_model.iloc[0,2],
                    'trainedAt': datetime.utcnow(),
                    'features' : mlArtifacts_base_model.iloc[0,3]
                }
               }, upsert= True)

    return None

#! notuse
def cold_start_by_counytry_modeling(client,
                                    saved_model = 'mlArtifacts_country',
                                    model_name = 'xgboost',
                                    based_model = 'th',
                                    updatedAtThreshold = 30.0):
"""
    # import package
    import pandas as pd
    import xgboost as xgb
    import pickle
    from datetime import datetime
    from pprint import pprint
    import iso3166 
    from datetime import datetime, timedelta
    
    # connnect to database

    appDb = client['app-db']
    analyticsDb = client['analytics-db']
    
    mlArtifacts_country = analyticsDb[saved_model]
    
    # define cursor of engagement transaction

    def prepare_features_country(updatedAtThreshold: float,
                     app_db: str,
                     engagement_collection: str): 
        """
   #     Query and then prepare features from engagement collection
        """
    
        transactionEngagementsCountry = [
            {
            # filter age of contents for only newer than specific days
                '$match': {
                    'updatedAt': {
                        '$gte': (datetime.utcnow() - timedelta(days=updatedAtThreshold))
                    }
                }
            }, {
            # join with 'app-db.users' for account id
                '$lookup': {
                    'from': 'users', 
                    'localField': 'user', 
                    'foreignField': '_id', 
                    'as': 'users'
                }
            }, {
            # deconstruct array => object format
                '$unwind': {
                    'path': '$users'
                }
            }, {
            # join with 'app-db.accounts' of country code
                '$lookup': {
                    'from': 'accounts', 
                    'localField': 'users.ownerAccount', 
                    'foreignField': '_id', 
                    'as': 'accounts'
                }
            }, {
            # deconstruct array => object format
                '$unwind': {
                    'path': '$accounts'
                }
            }, {
            # group by content id & country code
                '$group': {
                    '_id': {
                        'contentId': '$targetRef.$id', 
                        'countryCode': '$accounts.geolocation.countryCode'
                    }, 
                    'engangements': {
                        '$push': '$type'
                    }
                }
            }, {
            # deconstruct array => object format
                '$unwind': {
                    'path': '$engangements'
                }
            }, {
            # convert engagement to integer type
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
            # group by again but sum engagement
                '$group': {
                    '_id': '$_id', 
                    'like': {
                        '$sum': '$like'
                    }, 
                    'comment': {
                        '$sum': '$comment'
                    }, 
                    'recast': {
                        '$sum': '$recast'
                    }, 
                    'quote': {
                        '$sum': '$quote'
                    }
                }
            }, {
            # map output format
                '$project': {
                    '_id': 0, 
                    'contentId': '$_id.contentId', 
                    'countryCode': '$_id.countryCode', 
                    'like': 1, 
                    'comment': 1, 
                    'recast': 1, 
                    'quote': 1
                }
            }
        ]
        # assign result to dataframe
        transaction_engagements_country = pd.DataFrame(list(
            client[app_db][engagement_collection]\
                .aggregate(transactionEngagementsCountry))
            )
    
        return transaction_engagements_country

    # main logics
    transaction_engagements_country = prepare_features_country(
            updatedAtThreshold = updatedAtThreshold, # number of days to keep content
            app_db = 'app-db',
            engagement_collection = 'engagements')
    transaction_engagements_country['countryCode'] = \
        transaction_engagements_country['countryCode'].str.lower()

    contentFeatures = prepare_features(client = client, # default
                                        analytics_db = 'analytics-db',
                                        content_stats_collection = 'contentStats',
                                        creator_stats_collection = 'creatorStats')
    

    contentFeatures_clean = contentFeatures.fillna(0)
    transaction_engagements_with_contentFeatures = transaction_engagements_country.merge(contentFeatures_clean, on = 'contentId',how ='left')

    transaction_engagements_with_contentFeatures['label'] = (transaction_engagements_with_contentFeatures['like']
                                                              +transaction_engagements_with_contentFeatures['comment']
                                                              +transaction_engagements_with_contentFeatures['recast']
                                                              +transaction_engagements_with_contentFeatures['quote'])  
    
    # select country that can be trianed
    
    aggregated_transaction_engagements = transaction_engagements_with_contentFeatures.groupby('countryCode')['contentId'].agg('count').reset_index()
    selected_country = aggregated_transaction_engagements[aggregated_transaction_engagements['contentId'] > 2]

    # define save model artifact to database function
    def save_model_to_mongodb(collection, model_name, account, model):
    
        pickled_model = pickle.dumps(model) # pickling the model
    
        document = collection.update_one(
           {
            'account': account,
            'model': str(model_name),
           }, {
            '$set': {
                'account': account,
                'model': str(model_name),
                'artifact': pickled_model,
                'trainedAt': datetime.utcnow(),
                'features' : list(features.columns)
            }
           }, upsert= True)
"""        
    ml_artifacts = [] # pre-define model artifacts
    
    # loop through trainable country of selected country
    for n in list(selected_country.countryCode.unique()):
        
        # filter for only selected country
        selected_transaction_engagements = transaction_engagements_with_contentFeatures[transaction_engagements_with_contentFeatures['countryCode'] == n]  
        
        # summary engagements of selected country
        country_weight = selected_transaction_engagements.groupby('countryCode').agg( 
                                like_count = ('like','sum'),
                                comment_count = ('comment','sum'),
                                recast_count = ('recast','sum'),
                                quote_count = ('quote','sum')
                                                ).reset_index().replace(0,1)
        
         # formalize label
        country_weight = country_weight[['like_count','comment_count','recast_count','quote_count']].div(country_weight.sum(axis=1)[0]).div(-1)+1
        selected_transaction_engagements.loc[:,'like'] = selected_transaction_engagements.loc[:,'like']*country_weight.loc[0,'like_count']
        selected_transaction_engagements.loc[:,'comment'] = selected_transaction_engagements.loc[:,'comment']*country_weight.loc[0,'comment_count']
        selected_transaction_engagements.loc[:,'recast'] = selected_transaction_engagements.loc[:,'recast']*country_weight.loc[0,'recast_count']
        selected_transaction_engagements.loc[:,'quote'] = selected_transaction_engagements.loc[:,'quote']*country_weight.loc[0,'quote_count']
        selected_transaction_engagements['label'] = (selected_transaction_engagements['like']
                                                     +selected_transaction_engagements['comment']
                                                     +selected_transaction_engagements['recast']
                                                     +selected_transaction_engagements['quote'])  
        
        # separate features & label
        features = selected_transaction_engagements.drop(['label','countryCode','contentId','like','comment','recast','quote'],axis = 1)
        label  = selected_transaction_engagements.label

        # define estimator
        xgboost_model = xgb.XGBRegressor(random_state = 123)
        
        # Fit to the model 
        xgboost_model.fit(features, label)
    
        pprint(n)
        
        # append result
        ml_artifacts.append(xgboost_model) # collect list of artifacts
    
        # upsert 
        save_model_to_mongodb(collection=mlArtifacts_country,
                          account=n,
                          model_name = model_name,
                          model=xgboost_model) 
        
    # add Base model
    mlArtifacts = pd.DataFrame(list(mlArtifacts_country.find()))
    mlArtifacts_base_model = mlArtifacts[mlArtifacts['account'] == based_model].drop(['account'],axis = 1)
    
    # get country list
    country_list = list(set([x.lower() for x in iso3166.countries_by_alpha2.keys()]).difference(set(selected_country['countryCode'])))
    
    # loop for all country
    for i in country_list:
    # update collection  
        pprint(i)
        mlArtifacts_country.update_one(
               {
                'account': i,
                'model': mlArtifacts_base_model.iloc[0,1],
               }, {
                '$set': {
                    'account': i,
                    'model': str(mlArtifacts_base_model.iloc[0,1]),
                    'artifact': mlArtifacts_base_model.iloc[0,2],
                    'trainedAt': datetime.utcnow(),
                    'features' : mlArtifacts_base_model.iloc[0,3]
                }
               }, upsert= True)
    
    return None
        
def coldstart_train_main(mongo_client):

    transaction_engagements_with_contentFeatures_df = extract_data(mongo_client)

    transaction_engagements_with_contentFeatures_df, selected_country_df = \
        transform_data(transaction_engagements_with_contentFeatures_df)

    modeling_and_saving(
        transaction_engagements_with_contentFeatures_df, selected_country_df,
        mongo_client
    )
"""   
    cold_start_by_counytry_modeling(mongo_client,
                                    saved_model = 'mlArtifacts_country',
                                    model_name = 'xgboost',
                                    based_model = 'th',
                                    updatedAtThreshold = 30.0) 
 """

    
    return