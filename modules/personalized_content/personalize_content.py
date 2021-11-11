from bson.objectid import ObjectId

def personalized_content_modeling(input_engagement = 'transactionEngagements',
                                    saved_model = 'mlArtifacts',
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
    from bson.objectid import ObjectId   
    connectionUri = 'mongodb+srv://analytics-admin:pnYT55BGWwHePK1M@dev-cluster.fg2e5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
    client = pymongo.MongoClient(connectionUri)
 
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
    
    select_user = trans_add.groupby('userId')['contentId'].agg('count').reset_index()
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

    for n in list(select_user.userId.unique()):
    
        focus_trans = trans_add[trans_add['userId'] == n]  
        portion = focus_trans.groupby('userId').agg( 
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

        Xlr = focus_trans.drop(['label','userId','contentId','like','comment','recast','quote'],axis = 1)
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
    return None

def personalized_content_scroing( saved_model = 'mlArtifacts',
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

    connectionUri = 'mongodb+srv://analytics-admin:pnYT55BGWwHePK1M@dev-cluster.fg2e5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
    client = pymongo.MongoClient(connectionUri)

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

def personalized_content_main(user_id: ObjectId):

#	personalized_content_modeling(input_engagement = 'transactionEngagements',
#										saved_model = 'mlArtifacts',
#										content_features = 'contentFeatures',
#										model_name = 'xgboost')
    print('inside main[D]:', user_id, repr(user_id), type(user_id))
#    if (user_id == ObjectId('614addffec903a9d987eb580')) and (isinstance(user_id, ObjectId)):
    if isinstance(user_id, ObjectId):
        print("[INFO]: user_id OK!")
    personalized_content_scroing( saved_model = 'mlArtifacts',
                                    content_features = 'contentFeatures',
                                    userId = user_id,
                                    model_name = 'xgboost')
    
    return personalized_content_scroing