import logging

def cold_start_by_counytry_modeling(client,
                                    input_engagement = 'transactionEngagements_country2',
                                    saved_model = 'mlArtifacts_country',
                                    content_features = 'contentFeatures',
                                    model_name = 'xgboost'):
    import pymongo # connect to MongoDB
    from pymongo import MongoClient # client connection to MongoDB
    import sklearn
    import pandas as pd
    import json
    import xgboost as xgb
    import bson.objectid
    import pickle
    from datetime import datetime
    from pprint import pprint
    import numpy as np
    
    logging.info('Starting')
    
    appDb = client['app-db']
    analyticsDb = client['analytics-db']
    users = appDb['users']
    user = pd.DataFrame(list(users.find()))
    trans = analyticsDb[input_engagement]
    trans = pd.DataFrame(list(trans.find()))
    contentFeatures = analyticsDb[content_features]
    contentFeatures = pd.DataFrame(list(contentFeatures.find()))
    mlArtifacts_country = analyticsDb[saved_model]
    
    # debug
    print(contentFeatures.columns)
    
    contentFeatures_1 = contentFeatures.fillna(0).rename({'_id':'contentId'},axis = 1).drop('userId',axis = 1)
    trans_add = trans.merge(contentFeatures_1, on = 'contentId',how ='left')
    trans_add['label'] = trans_add['like']+trans_add['comment'] +trans_add['recast'] +trans_add['quote']  
    trans_add = trans_add.drop(['_id'],axis = 1)
    
    select_user = trans_add.groupby('countryId')['contentId'].agg('count').reset_index()
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
                'trainedAt': datetime.now(),
                'features' : list(Xlr.columns)
            }
           }, upsert= True)
    ml_artifacts = [] # pre-define model artifacts

    for n in list(select_user.countryId.unique()):
    
        focus_trans = trans_add[trans_add['countryId'] == n]  
        portion = focus_trans.groupby('countryId').agg( 
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

        Xlr = focus_trans.drop(['label','countryId','contentId','like','comment','recast','quote'],axis = 1)
        ylr = focus_trans.label

        xg_reg = xgb.XGBRegressor(random_state = 123)
        xg_reg.fit(Xlr, ylr)
    
        pprint(n)
        ml_artifacts.append(xg_reg) # collect list of artifacts
    
        # upsert 
        save_model_to_mongodb(collection=mlArtifacts_country,
                          account=n,
                          model_name= model_name,
                          model=xg_reg) 
        
        logging.info('Model Saved to MongoDB')
        return None

def coldstart_main(client, 
                   model_save_cllctn='mlArtifacts_country_test', 
                   countryId: list=['CH', 'EN', 'GER', 'LA', 'PHI', 'SP', 'TH', 'USA', 'VET'], 
                   model_name='xgboost', content_features='contentFeatures',
                   input_engagement='transactionEngagements_country2'):
    
    # 1 train move to new lambda
    cold_start_by_counytry_modeling(client,
        input_engagement = input_engagement,
        saved_model = model_save_cllctn,
        content_features = content_features,
        model_name = model_name)
    logging.info('coldstart trained')
    
    return None