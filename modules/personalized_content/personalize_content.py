import pandas as pd

def load_model_from_mongodb(collection, model_name, account):
    import pickle
    json_data = {}
        
    # find user's model artifact
    data = collection.find({
        'account': account,
        'model': model_name
    })
    
    for i in data:
        json_data = i
    
    pickled_model = json_data['artifact']
    
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
    
     #
    collection = db[collection_name]
    content_feature = db[content_features]

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