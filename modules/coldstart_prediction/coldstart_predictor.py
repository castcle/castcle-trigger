import logging

def cold_start_by_counytry_scroing( client,
                                    saved_model = 'mlArtifacts_country',
                                    saved_data = 'saved_prediction_country',
                                    content_features = 'contentFeatures',
                                    countryId = 'TH',
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
    
    logging.info('Country Scoring Start')
    appDb = client['app-db']
    analyticsDb = client['analytics-db']

    contentFeatures = analyticsDb[content_features]
    contentFeatures = pd.DataFrame(list(contentFeatures.find()))

    mlArtifacts_country = analyticsDb[saved_model]
    
    saved_data_country = analyticsDb[saved_data]


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

    xg_reg_load = load_model_from_mongodb(collection=mlArtifacts_country,
                                 account= countryId,
                                 model_name= model_name)

    result = pd.DataFrame()
    content_test = contentFeatures.drop(['userId'], axis = 1)

    a = pd.DataFrame(xg_reg_load.predict(content_test.drop(['_id'], axis = 1)), columns = ['predict'])
    b = contentFeatures[['_id']].reset_index(drop = True)
    c = pd.concat([b,a],axis =1)
    c['countryId'] = countryId
    c['Score_At'] = datetime.now() 
    c = c.sort_values(by='predict', ascending=False)
    result = result.append(c)  

    result.reset_index(inplace=False)
    data_dict = result.to_dict("records")
    # update collection 
    saved_data_country.update_one({'countryId': countryId},{'$set':{"scoring_list":data_dict}},upsert= True)
    
    logging.info('Country Scoring Done')
    return result

def coldstart_ret(country_scoring_result, head):
    from bson import ObjectId
    import json
    class JSONEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, ObjectId):
                return str(o)
            return json.JSONEncoder.default(self, o)
    
    country_scoring_result = country_scoring_result.head(head)
    
    contents_res = {}
    row_num = 0
    for index, row in country_scoring_result.iterrows():
        _id_str = row[0]
        _predict = str(row[1])
        _countryId = str(row[2])
        
        contents_res[row_num] = {
            '_id': _id_str,
            'predict_score': _predict,
            'countryId': _countryId
        }
        
        row_num+=1
        
    contents_res = JSONEncoder().encode(contents_res)
    
    return contents_res

def coldstart_predictor_main(client, model_save_cllctn='mlArtifacts_country', 
                   countryId: list=['CH', 'EN', 'GER', 'LA', 'PHI', 'SP', 'TH', 'USA', 'VET'], 
                   model_name='xgboost', content_features='contentFeatures',
                   input_engagement='transactionEngagements_country2'):
    
    # 2 predict
    country_scoring_result = cold_start_by_counytry_scroing(client,
                                saved_model = model_save_cllctn,
                                content_features = content_features,
                                countryId = countryId,
                                model_name = model_name)
    
    logging.info('coldstart trained')
    
    return None