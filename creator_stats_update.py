'''
creator statistics update
function
    update statistics of content creator users from their contents activities then upsert into database every cron(23 * * * ? *)
'''
import json
import sys
from mongo_client import mongo_client, ping_mongodb

def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    from modules.update_creator_stats.update_creator_stats import update_creator_stats_main

    print('update content creator statistics start')

    # call modules main function
    update_creator_stats_main(src_database_name='app-db',
                              src_collection_name='contents',
                              dst_database_name='analytics-db',
                              dst_collection_name='creatorStats',
                              contentDateThreshold=30.0, # day unit
                              likedWeight=1.0,
                              commentedWeight=1.0,
                              recastedWeight=1.0,
                              quotedWeight=1.0,
                              followedWeight=0.01,
                              halfLifeHours=24.0)

    print('update content creator statistics end')

    return None

    