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
import os
import random

def polularity_database(mongo_client):
  mycol_engagements = mongo_client['app-db']['contents']
    # agg engagements and contents, then group by total_type, total_user to send informayion to api 
  query_data_content = list(mycol_engagements.aggregate([
            {'$match': { 'visibility': "publish" }},
            {'$match':{'$expr': {'$lt': ["$createdAt",
                              { '$dateSubtract': { 'startDate': "$$NOW", 'unit': "day", 'amount': 7 }}]}}},
            {'$addFields': {'comment': '$engagements.comment.count','like': '$engagements.like.count','quote': '$engagements.quote.count','recast': '$engagements.recast.count',}},
            {'$project': {'_id' : 1, 'author':1,'comment':1 , 'like':1 ,  'quote':1 ,  'recast':1 , 'createdAt':1,'type':1,'isRecast':1,'originalPost':1}},
            { 
              '$addFields': { 
                  'originalId': { 
                    '$cond': [ 
                        
                        { '$eq': [ "isRecast", True] }
                        , '$originalPost._id' , '$_id' 
                    ] 
                  } 
              } 
            },
            {'$lookup': {
                        'from': 'users',
                        'localField': 'author.id',
                        'foreignField': '_id',
                        'as': 'user_relationship_all'
                    }},
            {'$project': {'_id' : 1 ,'user_relationship_all.ownerAccount':1,'createdAt':1,'type':1,'payload':1,'comment':1 , 'like':1 ,  'quote':1 ,  'recast':1 ,'originalId':1}},
            {'$lookup': {
                  'from': 'accounts',
                  'localField': 'user_relationship_all.ownerAccount',
                  'foreignField': '_id',
                  'as': 'content_relationship_all'
              }},
              { '$addFields': {'message': '$payload.message','continentCode': '$content_relationship_all.geolocation.continentCode','countryCode': '$content_relationship_all.geolocation.countryCode'}},
              {'$project': {'_id' : 1 ,'author.id':1,'createdAt':1,'type':1,'message':1,'continentCode':1,'countryCode':1,'comment':1 , 'like':1 ,  'quote':1 ,  'recast':1,'originalId':1}}
          ])) 
  df = pd.DataFrame(query_data_content)
  df1=  df.copy()
  df1['comment'] =df1['comment'].mul(1.5)
  df1['like'] =df1['like'].mul(1.0)
  df1['quote'] =df1['quote'].mul(2.5)
  df1['recast'] =df1['recast'].mul(2.0)
  df1['continentCode'] = df1.loc[:,'continentCode'].apply(lambda x: str(x).strip("[']"))
  df1['countryCode'] = df1.loc[:,'countryCode'].apply(lambda x: str(x).strip("[']"))
  df1['eventStrength']= df1.loc[:,('comment', 'like','quote', 'recast')].sum(axis=1)
  item_popularity_df = df1[['_id','eventStrength','countryCode','createdAt','type','originalId']]
  item_popularity_df['score']= item_popularity_df['eventStrength'] / item_popularity_df.groupby('countryCode')['eventStrength'].transform('sum')
  item_popularity_df = item_popularity_df.round(6)
  return item_popularity_df
  

def suggest_services_default_content_main(mongo_client):
  df = polularity_database(mongo_client)
  updates = []
  for _, row in df.iterrows():
      updates.append(UpdateOne({'_id': row.get('_id')}, {'$set': {'eventStrength': row.get('eventStrength'),'countryCode': row.get('countryCode'),'createdAt': row.get('createdAt'),'type': row.get('type'),'score': row.get('score'),'originalId': row.get('originalId')}}, upsert=True))
  
  col = mongo_client['analytics-db']['default_guest']
  col.bulk_write(updates)
  return None
