from bson.objectid import ObjectId

def personalized_content_scroing(client,
                                 saved_model = 'mlArtifacts',
                                    saved_data = 'saved_prediction',
                                    content_features = 'contentFeatures',
                                    content_fillter = 'contentFillter',
                                    userId = '',
                                    model_name = 'xgboost'):
    
    import pymongo # connect to MongoDB
    from pymongo import MongoClient # client connection to MongoDB
    import sklearn
    import pandas as pd
    import json
    import xgboost as xgb
    from bson.objectid import ObjectId
    import pickle
    from datetime import datetime
    from pprint import pprint
    import numpy as np

    appDb = client['app-db']
    analyticsDb = client['analytics-db']

    contentFeatures = analyticsDb[content_features]
    contentFeatures = pd.DataFrame(list(contentFeatures.find({ })))
    
    contentFillter = analyticsDb[content_fillter]
    contentFillter = pd.DataFrame(list(contentFillter.find({ })))

    mlArtifacts = analyticsDb[saved_model]
    
    saved_data = analyticsDb[saved_data]

    def load_model_from_mongodb(collection, model_name, account):
        json_data = {}
        data = collection.find({
            'account': account,
            'model': model_name
        })
    
        for i in data:
            json_data = i
    
        pickled_model = json_data['artifact']
    
        return pickle.loads(pickled_model)

    xg_reg_load = load_model_from_mongodb(collection=mlArtifacts,
                                 account= userId,
                                 model_name= model_name)

    result = pd.DataFrame()
    contentFillter = contentFillter[contentFillter['userId'] == userId].drop(['userId','_id'],axis = 1)
    content_test = contentFeatures.drop(['userId'], axis = 1)
    content_test = content_test.merge(contentFillter,left_on = '_id', right_on= 'contentId' ,how = 'inner')

    a = pd.DataFrame(xg_reg_load.predict(content_test.drop(['_id','contentId'], axis = 1)), columns = ['predict'])
    b = content_test[['_id']].reset_index(drop = True)
    c = pd.concat([b,a],axis =1)
    c['userId'] = userId
    c['Score_At'] = datetime.now() 
    c = c.sort_values(by='predict', ascending=False)
    result = result.append(c)
    
    result.reset_index(inplace=False)
    data_dict = result.to_dict("records")
    # update collection 
    saved_data.update_one({'userId': userId},{'$set':{"scoring_list":data_dict}},upsert= True)
    
    return None

def personalized_content_main(client,
                              user_id: ObjectId):

    if isinstance(user_id, ObjectId):
        print("[INFO]: user_id OK!")

    personalized_content_scroing(client,
                                 saved_model = 'mlArtifacts',
                                    content_features = 'contentFeatures',
                                    userId = user_id,
                                    model_name = 'xgboost')
    
    return None