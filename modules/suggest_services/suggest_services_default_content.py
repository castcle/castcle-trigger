import joblib
import pandas as pd
from flask import Flask, request, Response,make_response
from flask import abort, render_template, request
from pymongo import MongoClient , UpdateOne
import pandas as pd
import logging
from logging.config import dictConfig
import json
from mongo_client import mongo_client
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
#                       {'$match':{'$expr': {'$gte': ["$createdAt",
#                                   { '$dateSubtract': { 'startDate': "$$NOW", 'unit': "day", 'amount': 14 }}]}}},
                      {'$project': {'_id' : 1 ,'author.id':1,'createdAt':1,'type':1,'payload':1,'originalId':1}},
                      {'$lookup': {
                          'from': 'users',
                          'localField': 'author.id',
                          'foreignField': '_id',
                          'as': 'user_relationship_all'
                      }},
                      {'$project': {'_id' : 1 ,'user_relationship_all.ownerAccount':1,'createdAt':1,'type':1,'payload':1,'originalId':1}},
                      {'$lookup': {
                          'from': 'accounts',
                          'localField': 'user_relationship_all.ownerAccount',
                          'foreignField': '_id',
                          'as': 'content_relationship_all'
                      }},
                      # {'$unwind': '$content_relationship_all'},
                      { '$addFields': {'message': '$payload.message','continentCode': '$content_relationship_all.geolocation.continentCode','countryCode': '$content_relationship_all.geolocation.countryCode'}},
                      # {'$unwind': '$content_relationship_all'},
                      {'$project': {'_id' : 1 ,'createdAt':1,'type':1,'message':1,'continentCode':1,'countryCode':1,'originalId':1}},  

  ])) 
  df = pd.DataFrame(query_content)
  df = df.rename(columns = {'_id':'contentId', 'type': 'content_type','message':'message','createdAt':'createdAt','continentCode':'continentCode','countryCode':'countryCode','originalId':'originalId'})
#   df.columns = ['contentId','content_type','createdAt','message','continentCode','countryCode']
  list_ob =list([d['_id'] for d in query_content])
  mycol_default_guest= client['analytics-db']['contentfiltering']
      # agg engagements and contents, then group by total_type, total_user to send informayion to api 
  query_default_guest = list(mycol_default_guest.aggregate([
                        # match ตาม condition : countryCode & languages
                        { '$match': {'contentId': { '$in': list_ob }}},
                      { '$addFields': {'class': '$topic_classify.class'}},
                        {'$project': {'contentId' : 1,'language': 1 ,'class':1}},
  #                       { '$sort': { 'score': -1 } },
                        # { '$limit': maxResults}
                        ]))  
  more_content= pd.DataFrame(query_default_guest)
  dat_final= df.merge(more_content, on='contentId', how='left')
  return dat_final

def precess_ingest(articles_df: pd.DataFrame,interactions_df: pd.DataFrame):
  # clean articles_df
  articles_df.columns = ['contentId', 'content_type', 'createdAt', 'title', 'continentCode','countryCode','_id','language','class']
  articles_df = articles_df.dropna(subset = ['title','_id'])
  articles_df['contentId']= articles_df.loc[:,'contentId'].apply(lambda x: ''.join(str(x)))

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
  # {'comment', 'like', 'quote', 'recast', 'report'}
  interactions_indexed_df['eventStrength'] = interactions_indexed_df['eventtype'].apply(lambda x: event_type_strength[x])

  users_interactions_count_df = interactions_indexed_df.groupby(['personId', 'contentId']).size().groupby('personId').size()
  print('# users: %d' % len(users_interactions_count_df))
  users_with_enough_interactions_df = users_interactions_count_df[users_interactions_count_df >= 4].reset_index()[['personId']]
  print('# users with at least 4 interactions: %d' % len(users_with_enough_interactions_df))


  print('# of interactions: %d' % len(interactions_indexed_df))
  interactions_from_selected_users_df = interactions_indexed_df.merge(users_with_enough_interactions_df, 
                how = 'right',
                left_on = 'personId',
                right_on = 'personId')
  print('# of interactions from users with at least 5 interactions: %d' % len(interactions_from_selected_users_df))

  interactions_full_df = interactions_from_selected_users_df \
                    .groupby(['personId', 'contentId']).agg({'timestamp':  'max', 'eventStrength': 'sum'}) \
                    .reset_index()
  print('# of unique user/item interactions: %d' % len(interactions_full_df))


  contentId_list = list(set(interactions_full_df['contentId'].tolist()))
  con_data = articles_df[articles_df.contentId.isin(contentId_list)].reset_index(drop=True)
  con_data['class']= con_data['class'].fillna('etc')

  return interactions_full_df, con_data

def process_na(interactions_full_df: pd.DataFrame,con_data: pd.DataFrame):
  train = interactions_full_df.merge(con_data[['contentId', 'content_type', 'createdAt', 'title', 'continentCode','countryCode', 'language', 'class']], how="left", on='contentId')
  train['countryCode'] = train.loc[:,'countryCode'].apply(lambda x: str(x).replace("[]",'us'))
  train['countryCode'] = train.loc[:,'countryCode'].apply(lambda x: str(x).strip("[']"))
  train['continentCode'] = train.loc[:,'continentCode'].apply(lambda x: str(x).replace("[]",'us'))
  train['continentCode'] = train.loc[:,'continentCode'].apply(lambda x: str(x).strip("[']"))
  train['language'] = train['language'].fillna('en')
  return train

def Popularity(train: pd.DataFrame, condition: str):
  #Computes the most popular items
  train_C = train.groupby(['contentId','title',f'{condition}']).agg({'timestamp':  'max', 'eventStrength': 'sum'}).sort_values(ascending=False,by=['eventStrength']).reset_index()
  columnsC = [f'{condition}']
  train_C['model'] = train_C[columnsC].to_dict(orient='records')
  train_C['updatedAt'] = pd.Timestamp.now()  
  # ก่อนทำต้องเลือกข้อมูลมาก่อน
  scaler = MinMaxScaler()
  train_C['score'] = scaler.fit_transform(train_C[['eventStrength']])
  return train_C

def suggest_services_default_content_main(mongo_client):
  interactions_df= query_engage(mongo_client)

  articles_df= query_contents(mongo_client)

  interactions_full_df, con_data = precess_ingest(articles_df=articles_df,interactions_df=interactions_df)

  train= process_na(interactions_full_df, con_data)

  train_C = Popularity(train, condition= 'countryCode')

  train_L = Popularity(train, condition= 'language')

  list_col_for_final = ['contentId', 'title', 'timestamp', 'eventStrength','model', 'score']

  final =  pd.concat([train_C[list_col_for_final],train_L[list_col_for_final]]).reset_index(drop=True)
  final['updatedAt'] = pd.Timestamp.now()  
  final['score'] = final['score'].round(4)
  df = final
  updates = []
  for _, row in df.iterrows():
      updates.append(UpdateOne({'_id': row.get('contentId')}, {'$set': {'contentId': row.get('contentId'),'title': row.get('title'),'timestamp': row.get('timestamp'),'eventStrength': row.get('eventStrength'),'model': row.get('model'),'score': row.get('score'),'updatedAt': row.get('updatedAt')}}, upsert=True))
  col = mongo_client['analytics-db']['default_guest']
  col.bulk_write(updates)
  return None
