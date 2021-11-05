def load_model_from_mongodb(collection, model_name, account):
    
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

def personalized_content():
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

    # connect to MongoDB
    ## define connection URI as role; analytics-admin
    connectionUri = 'mongodb+srv://analytics-admin:pnYT55BGWwHePK1M@dev-cluster.fg2e5.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'

    ## assign client
    client = pymongo.MongoClient(connectionUri)

    ## assign databases
    appDb = client['app-db']
    analyticsDb = client['analytics-db']

    ## USERS download
    users = appDb['users']

    # explore schema
    user = pd.DataFrame(list(users.find()))

    ## creator download
    contentFeatures = analyticsDb['contentFeatures']

    contentFeatures = pd.DataFrame(list(contentFeatures.find()))

    mlArtifacts = analyticsDb['mlArtifacts']

    return