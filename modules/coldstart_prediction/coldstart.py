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

def coldstart_main(client, model_save_cllctn='mlArtifacts_country', countryId=['CH', 'EN', 'GER', 'LA', 'PHI', 'SP', 'TH', 'USA', 'VET'], 
                   model_name='xgboost', content_features='contentFeatures',
                   input_engagement='transactionEngagements_country2'):
    
    # 1 train
    cold_start_by_counytry_modeling(client,
        input_engagement = input_engagement,
        saved_model = model_save_cllctn,
        content_features = content_features,
        model_name = model_name)
    
    # 2 predict
    for country in countryId:
        cold_start_by_counytry_scroing(client,
                                        saved_model = model_save_cllctn,
                                        content_features = content_features,
                                        countryId = country,
                                        model_name = model_name)
    
    # 3 return result in json format
    #country_scoring_res_json = coldstart_ret(country_scoring_result, head=100)
    
    logging.info('Country scoring done')
    
    return None