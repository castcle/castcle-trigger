import json
from mongo_client import mongo_client
import pymongo # connect to MongoDB
from datetime import datetime, timedelta
from pprint import pprint
import random

db = mongo_client['analytics-db']
contents = db['testContents']

def handle(event, context):
    print(json.dumps(event, indent=4))
    
    
    
print('who I am?')
# all accounts are assumed to follow each other
# considering on contents collection

# parseHastagCursor = [
#     {
#         # filter for newer than 14 days
#         # in mongodb use 'new Date(ISODate().getTime() - 1000*60*60*24*14' instead
#         '$match': {
#             "createdAt": {
#                 '$gte': (datetime.now() - timedelta(days=14)) 
#                      }
#         }
#     },
#     {
#         '$sort': {"updatedAt": -1}
#     },
#     {
#         # <- limited at 100 MB
#         '$group': {
#             '_id': "$hashtags.payload",
#             "hashtagCount": {'$count': {}},
#             "latestPost": {'$max': "$createdAt"},
#             # "latestPost": {'$min': "$createdAt"},
#             "taggedContent": {
#                 '$push': {
#                     "_id": "$_id",
#                     "updatedAt": "$updatedAt"
#                 }
#             }
#         }
#     }, 
#     {
#         '$sort': {"hashtagCount": -1}
#     },
#     # {
#     #     # upsert to collection name: testHashtagStat
#     #     '$merge': { 
#     #         'into': {'db': "analytics-db", 'coll': "testHashtagStat"},
#     #         'on': "_id",
#     #         'whenMatched': "replace", 
#     #         'whenNotMatched': "insert"
#     #     } 
#     # }
# ]

# # print output
# pprint(list(contents.aggregate(parseHastagCursor)))

contents.find_one()
