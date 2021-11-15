# This file updates collection 'creatorStats' from 'contents'
import json
import sys
from mongo_client import mongo_client, ping_mongodb

def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    # print(json.dumps(event, indent=4)) # print event
    from modules.update_creator_stats.update_creator_stats import update_creator_stats_main

    # define content parameters
    contentDateThreshold = 14
    likedWeight = 1
    commentedWeight = 1
    recastedWeight = 1
    quotedWeight = 1
    followedWeight = 0.01
    halfLifeHours = 24

    update_creator_stats_main(src_database_name=src_database_name,
                              src_collection_name=src_collection_name,
                              contentDateThreshold=contentDateThreshold,
                              likedWeight=likedWeight,
                              recastedWeight=recastedWeight,
                              quotedWeight=quotedWeight,
                              followedWeight=followedWeight,
                              halfLifeHours=halfLifeHours)

    print('update content creator statistics done')

    