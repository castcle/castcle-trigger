def cold_start_by_counytry_modeling(client,
                                    saved_model = 'mlArtifacts_country',
                                    model_name = 'xgboost',
                                    based_model = 'th',
                                    updatedAtThreshold = 30.0):    
    import pandas as pd
    import xgboost as xgb
    import pickle
    from datetime import datetime
    from pprint import pprint
    import iso3166    
    
 
    appDb = client['app-db']
    analyticsDb = client['analytics-db']

    def prepare_features_country(updatedAtThreshold: float,
                     app_db: str,
                     engagement_collection: str):
        from datetime import datetime, timedelta
        
    
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
    
        transaction_engagements_country = pd.DataFrame(list(client[app_db][engagement_collection].aggregate(transactionEngagementsCountry)))
    
        return transaction_engagements_country

    trans = prepare_features_country(updatedAtThreshold = updatedAtThreshold, # number of days to keep content
                                      app_db = 'app-db',
                                      engagement_collection = 'engagements')
    trans['countryCode'] = trans['countryCode'].str.lower()
    
    def prepare_features(client, 
                     analytics_db: str,
                     content_stats_collection: str,
                     creator_stats_collection: str):
    
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

        content_features = pd.DataFrame(list(client[analytics_db][content_stats_collection].aggregate(contentFeaturesCursor))).rename({'_id':'contentId'},axis = 1)
    
        return content_features

    contentFeatures = prepare_features(client = client, # default
                                        analytics_db = 'analytics-db',
                                        content_stats_collection = 'contentStats',
                                        creator_stats_collection = 'creatorStats')
    
    
    mlArtifacts_country = analyticsDb[saved_model]

    contentFeatures_1 = contentFeatures.fillna(0)
    trans_add = trans.merge(contentFeatures_1, on = 'contentId',how ='left')
    trans_add['label'] = trans_add['like']+trans_add['comment'] +trans_add['recast'] +trans_add['quote']  
    trans_add = trans_add
    
    select_user = trans_add.groupby('countryCode')['contentId'].agg('count').reset_index()
    select_user = select_user[select_user['contentId'] > 2]

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
                'features' : list(Xlr.columns)
            }
           }, upsert= True)
    ml_artifacts = [] # pre-define model artifacts

    for n in list(select_user.countryCode.unique()):
    
        focus_trans = trans_add[trans_add['countryCode'] == n]  
        portion = focus_trans.groupby('countryCode').agg( 
                                like_count = ('like','sum'),
                                comment_count = ('comment','sum'),
                                recast_count = ('recast','sum'),
                                quote_count = ('quote','sum')
                                                ).reset_index().replace(0,1)
    
        portion = portion[['like_count','comment_count','recast_count','quote_count']].div(portion.sum(axis=1)[0]).div(-1)+1
        focus_trans.loc[:,'like'] = focus_trans.loc[:,'like']*portion.loc[0,'like_count']
        focus_trans.loc[:,'comment'] = focus_trans.loc[:,'comment']*portion.loc[0,'comment_count']
        focus_trans.loc[:,'recast'] = focus_trans.loc[:,'recast']*portion.loc[0,'recast_count']
        focus_trans.loc[:,'quote'] = focus_trans.loc[:,'quote']*portion.loc[0,'quote_count']
        focus_trans['label'] = focus_trans['like']+focus_trans['comment'] +focus_trans['recast'] +focus_trans['quote']  

        Xlr = focus_trans.drop(['label','countryCode','contentId','like','comment','recast','quote'],axis = 1)
        ylr = focus_trans.label

        xg_reg = xgb.XGBRegressor(random_state = 123)
        xg_reg.fit(Xlr, ylr)
    
        pprint(n)
        ml_artifacts.append(xg_reg) # collect list of artifacts
    
        # upsert 
        save_model_to_mongodb(collection=mlArtifacts_country,
                          account=n,
                          model_name = model_name,
                          model=xg_reg) 
        
    # add Base model
    mlArtifacts = pd.DataFrame(list(mlArtifacts_country.find()))
    mlArtifacts_base = mlArtifacts[mlArtifacts['account'] == based_model].drop(['account'],axis = 1)
    
    country_list = list(set([x.lower() for x in iso3166.countries_by_alpha2.keys()]).difference(set(select_user['countryCode'])))
    for i in country_list:
    # update collection  
        pprint(i)
        mlArtifacts_country.update_one(
               {
                'account': i,
                'model': mlArtifacts_base.iloc[0,1],
               }, {
                '$set': {
                    'account': i,
                    'model': str(mlArtifacts_base.iloc[0,1]),
                    'artifact': mlArtifacts_base.iloc[0,2],
                    'trainedAt': datetime.utcnow(),
                    'features' : mlArtifacts_base.iloc[0,3]
                }
               }, upsert= True)
    
    return None
        
def coldstart_train_main(client):
    
    cold_start_by_counytry_modeling(client,
                                    saved_model = 'mlArtifacts_country',
                                    model_name = 'xgboost',
                                    based_model = 'th',
                                    updatedAtThreshold = 30.0) 
    

    
    return