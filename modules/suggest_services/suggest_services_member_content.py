

# Instaling PyMongo, this is the interface to connect to MongoDB with Python
# ! python -m pip install pymongo==3.7.2
# ! python -m pip install pymongo[srv]
import tensorflow as tf
import warnings
warnings.filterwarnings("ignore")
from mongo_client import mongo_client
## for data
import pandas as pd
import numpy as np
import re
from datetime import datetime

## for machine learning
from sklearn import metrics, preprocessing

## for deep learning
from tensorflow.keras import models, layers, utils  #(2.6.0)

import pymongo
from pymongo import MongoClient
import pandas as pd
from bson.objectid import ObjectId
from sklearn.preprocessing import MinMaxScaler
# connnect to db

import pandas as pd
from pymongo import MongoClient , UpdateOne
import logging
from logging.config import dictConfig
import json
# from mongo_client import mongo_client
from bson.objectid import ObjectId
from sklearn.preprocessing import MinMaxScaler
import os
import random

def extract_dbref(x):
  from bson.dbref import DBRef
  return x.id

def query_engage(client):
  # retrive content 
  mycol_engagements = client['app-db']['engagements']
  query_engagements = list(mycol_engagements.aggregate([
                      {'$match':{'$expr': {'$gte': ["$createdAt",
                                  { '$dateSubtract': { 'startDate': "$$NOW", 'unit': "month", 'amount': 4 }}]}}},
                      {'$project': {'_id' : 1 ,'user' : 1 ,'targetRef':1,'type':1,'createdAt':1}}
  ])) 
  print(len(query_engagements))
  #create dateframe
  query_engagements = pd.DataFrame(query_engagements)


  #remove report row
  query_engagements = query_engagements.loc[~(query_engagements['type'] == 'report')]
  #extract value from DBref
  query_engagements['targetRef'] = query_engagements['targetRef'].apply(extract_dbref)
  query_engagements_final = query_engagements.rename(columns = {'_id':'sessionId', 'user': 'personId','targetRef':'contentId','createdAt':'timestamp','type':'eventtype'})
#   query_engagements.columns=  ['sessionId','eventtype','personId','contentId','timestamp']
  return query_engagements_final

def query_contents(client):
  mycol_engagements = client['app-db']['contents']
  query_content= list(mycol_engagements.aggregate([  
                      {'$match': { 'visibility': "publish" }},
#                       {'$match':{'$expr': {'$gte': ["$createdAt",
#                                   { '$dateSubtract': { 'startDate': "$$NOW", 'unit': "day", 'amount': 14 }}]}}},
                      {'$project': {'_id' : 1 ,'author.id':1,'createdAt':1,'type':1,'payload':1,'originalId':1}},
                      {'$lookup': {
                          'from': 'users',
                          'localField': 'author.id',
                          'foreignField': '_id',
                          'as': 'user_relationship_all'
                      }},
                      {'$project': {'_id' : 1 ,'user_relationship_all.ownerAccount':1,'createdAt':1,'type':1,'payload':1,'originalId':1,'author.id':1}},
                      {'$lookup': {
                          'from': 'accounts',
                          'localField': 'user_relationship_all.ownerAccount',
                          'foreignField': '_id',
                          'as': 'content_relationship_all'
                      }},
                      # {'$unwind': '$content_relationship_all'},
                      { '$addFields': {'message': '$payload.message','continentCode': '$content_relationship_all.geolocation.continentCode','countryCode': '$content_relationship_all.geolocation.countryCode','author':'$author.id' }},
                      # {'$unwind': '$content_relationship_all'},
                      {'$project': {'_id' : 1 ,'createdAt':1,'type':1,'message':1,'continentCode':1,'countryCode':1,'originalId':1,'author':1}},  

  ])) 
  df = pd.DataFrame(query_content)
  
  df = df.rename(columns = {'_id':'contentId'})
  list_ob =list([d['_id'] for d in query_content])
  mycol_default_guest= client['analytics-db']['contentfiltering']
      # agg engagements and contents, then group by total_type, total_user to send informayion to api 
  query_default_guest = list(mycol_default_guest.aggregate([
                        # match ตาม condition : countryCode & languages
                        { '$match': {'contentId': { '$in': list_ob }}},
                      { '$addFields': {'class': '$topic_classify.class'}},
                        {'$project': {'contentId' : 1,'language': 1 ,'class':1}},

                        ]))  
  more_content= pd.DataFrame(query_default_guest)
  dat_final= df.merge(more_content, on='contentId', how='left')
  return dat_final

def precess_ingest(articles_df: pd.DataFrame,interactions_df: pd.DataFrame):
  # clean articles_df
  articles_df = articles_df.rename(columns = {'contentId':'contentId', 'type': 'content_type','message':'title','createdAt':'createdAt','continentCode':'continentCode','countryCode':'countryCode','originalId':'originalId','author':'author'})
  articles_df = articles_df.dropna(subset = ['title','_id'])
  articles_df['contentId']= articles_df.loc[:,'contentId'].apply(lambda x: ''.join(str(x)))
  articles_df['author']= articles_df.loc[:,'author'].apply(lambda x: ''.join(str(x)))

  # clean interaction
  interactions_df['personId']= interactions_df['personId'].apply(lambda x: ''.join(str(x)))
  interactions_df['contentId']= interactions_df['contentId'].apply(lambda x: ''.join(str(x)))
  interactions_df = interactions_df.astype({'timestamp': 'datetime64[ns]'})

  interactions_indexed_df = interactions_df[interactions_df['contentId'] \
                                                  .isin(articles_df['contentId'])].set_index('personId')

  event_type_strength = {
  'comment': 1.5,
  'like': 1.0, 
  'quote': 2.5, 
  'recast': 2.0,
    }
  eventtype_list=  ['comment', 'like', 'quote', 'recast', 'report']
  # np.where(((df.A < borderE) & ((df.B - df.C) < ex)), 'r', 'b')
  interactions_indexed_df['eventtype'] = np.where(interactions_indexed_df['eventtype'].isin(eventtype_list),interactions_indexed_df['eventtype'],'like')
  interactions_indexed_df['y'] = interactions_indexed_df['eventtype'].apply(lambda x: event_type_strength[x])

  interactions_full_df = interactions_indexed_df \
                    .groupby(['personId', 'contentId']).agg({'timestamp':  'max', 'y': 'sum'}) \
                    .reset_index()
  # interactions_full_df = interactions_indexed_df
  print('# of unique user/item interactions: %d' % len(interactions_full_df))
  interactions_full_df = interactions_full_df.astype({'timestamp': 'datetime64[ns]'})
  interactions_full_df["daytime"] = interactions_full_df["timestamp"].apply(lambda x: 1 if 6<int(x.strftime("%H"))<20 else 0)
  interactions_full_df["weekend"] = interactions_full_df["timestamp"].apply(lambda x: 1 if x.weekday() in [5,6] else 0)


  contentId_list = list(set(interactions_full_df['contentId'].tolist()))
  con_data = articles_df[articles_df.contentId.isin(contentId_list)].reset_index(drop=True)
  con_data['class']= con_data['class'].fillna('etc')

  return interactions_full_df, con_data

# interactions_df= query_engage(mongo_client)

# articles_df= query_contents(mongo_client)

# interactions_full_df, con_data = precess_ingest(articles_df=articles_df,interactions_df=interactions_df)

# clean initial

def suggest_content_member_main(mongo_client):
    interactions_df= query_engage(mongo_client)

    articles_df= query_contents(mongo_client)

    interactions_full_df, con_data = precess_ingest(articles_df=articles_df,interactions_df=interactions_df)

    user = list(set(interactions_full_df.personId.tolist()))

    products = list(set(interactions_full_df.contentId.tolist()))

    interactions_full_df['user']= interactions_full_df['personId'].apply(lambda x: user.index(x))

    interactions_full_df['product']= interactions_full_df['contentId'].apply(lambda x: products.index(x))
    con_data['product']= con_data['contentId'].apply(lambda x: products.index(x))
    con_data= con_data.dropna(subset=['product'])
    con_data=con_data.drop_duplicates(subset=['product'])

    interactions_full_df

    #Creating a sparse pivot table with users in rows and items in columns
    users_items_pivot_matrix_df = interactions_full_df.pivot(index='user', 
                                                              columns='product', 
                                                              values='y')
    users_items_pivot_matrix_df.head(10)



    users_items_pivot_matrix_df.fillna(0, inplace= True)

    users_items_pivot_matrix_df = users_items_pivot_matrix_df.to_numpy()

    users_items_pivot_matrix_df.shape

    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import MinMaxScaler

    x_train, x_val,y_train,y_val = train_test_split(users_items_pivot_matrix_df,users_items_pivot_matrix_df , test_size =0.2, random_state=1)

    t =MinMaxScaler()
    t.fit(users_items_pivot_matrix_df)

    x_train = t.transform(x_train)
    x_val = t.transform(x_val)

    print(x_train.min(), x_train.max())
    print(x_val.min(), x_val.max())

    num_input = len(products)
    inp = tf.keras.layers.Input((num_input))
    e = tf.keras.layers.Dense(100)(inp)
    e = tf.keras.layers.BatchNormalization()(e)
    e = tf.keras.layers.LeakyReLU()(e)

    n_bottleneck =5
    bottleneck = tf.keras.layers.Dense(n_bottleneck)(e)

    d = tf.keras.layers.Dense(100)(bottleneck)
    d = tf.keras.layers.BatchNormalization()(d)
    d = tf.keras.layers.LeakyReLU()(d)

    decoded = tf.keras.layers.Dense(num_input, activation = 'sigmoid')(d)

    ae= tf.keras.models.Model(inp, decoded)
    # ae.summary()

    ae.compile(loss = 'mse',optimizer='adam')

    ModelCheckpoint = tf.keras.callbacks.ModelCheckpoint
    load_model = tf.keras.models.load_model

    filename = 'model.h5'
    checkpoint = ModelCheckpoint(filename, monitor='val_loss', verbose =1 , save_best_only=True, mode='min')

    history=ae.fit(x_train,x_train,epochs=10, batch_size=32,
                      validation_data =(x_val,y_val) , callbacks = [checkpoint], shuffle =True)


    preds = ae(users_items_pivot_matrix_df)

    preds = preds.numpy()

    preds_data = pd.DataFrame(preds)

    preds_df = preds_data.stack().reset_index()

    preds_df.columns = ['user','content', 'score']

    df = preds_df.reset_index()
    df['updatedAt'] = pd.Timestamp.now()  
    db = mongo_client['analytics-db']['results_recommend']
    db.delete_many({})
    db.insert_many(df.to_dict("records"))

    test_key = interactions_full_df[['personId','user']].drop_duplicates()
    df = test_key.reset_index(drop=True)
    df['updatedAt'] = pd.Timestamp.now()  
    db = mongo_client['analytics-db']['person_dict']
    db.delete_many({})
    db.insert_many(df.to_dict("records"))

    test_Content = interactions_full_df[['contentId','product']].drop_duplicates()
    test_Content.columns = ['contentId', 'content']
    df = test_Content.reset_index(drop=True)
    df['updatedAt'] = pd.Timestamp.now()  
    db = mongo_client['analytics-db']['content_dict']
    db.delete_many({})
    db.insert_many(df.to_dict("records"))

    df = con_data[['contentId','author','title']].reset_index()
    df['updatedAt'] = pd.Timestamp.now()  
    db = mongo_client['analytics-db']['content_info']
    db.delete_many({})
    db.insert_many(df.to_dict("records"))
    
    return print("contents Updated")
