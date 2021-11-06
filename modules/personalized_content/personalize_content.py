import pandas as pd

def load_model_from_mongodb(collection, model_name, account):
    import pickle
    json_data = {}
        
    #!
    print('collection', collection, type(collection))
    print('model_name', model_name, type(model_name))
    print('account', account, type(account))
    # find user's model artifact
    data = collection.find({
        'account': account,
        'model': model_name
    })
    #!
    print('data:', data, type(data))
    for i in data:
        json_data = i
    #!
    print(json_data)
    pickled_model = json_data['artifact']
    print('pickled:', pickled_model, type(pickled_model))
    return pickle.loads(pickled_model)

def personalized_content(db,
                         collection_name: str, 
                         content_features: str,
                         user_id):
    # import libraries
    import pymongo # connect to MongoDB
    from pymongo import MongoClient # client connection to MongoDB
    import sklearn
    import json
    import xgboost as xgb
    import bson.objectid
    from datetime import datetime
    from pprint import pprint
    import numpy as np
    
    print('db',db)
    print(type(db))
    print('collection_name',collection_name)
    print(type(collection_name))
    print('content_features',content_features)
    print(type(content_features))
    print('user_id',user_id)
    print(type(user_id))
    #
    collection = db[collection_name]
    print('collection:', collection, type(collection))
    content_feature = db[content_features]
    print('content_feature:',content_feature, type(content_feature))
    # convert objectid
    user_id = bson.objectid(user_id)
    # perform loading model
    xg_reg_load = load_model_from_mongodb(collection=collection,
                                 account=user_id,
                                 model_name='xgboost')
#	model_df = pd.DataFrame(list(.find({ "account" : user_id })))
    # perform loading features
    # 
    features = pd.DataFrame(list(content_feature.find({ "account" : user_id })))

    

    features = features.rename({'_id':'contentId'},axis = 1)\
        .drop(['contentId','userId'], axis = 1)
    result_df = pd.DataFrame(xg_reg_load.predict(features), columns = ['predict'])
    features_rnm_slced = features[['_id']]\
         .rename({'_id':'contentId'},axis = 1)\
        .reset_index(drop = True)
           
    result_feature_concat = pd.concat([features_rnm_slced,result_df],axis =1)
    result_feature_concat['userId'] = user_id
    result_feature_concat = result_feature_concat\
         .sort_values(by='predict', ascending=False)
    
    result_feature_concat_slice = result_feature_concat.head(100)
    
    print(result_feature_concat_slice)

    return result_feature_concat_slice