'''
content statistics update
function
    update statistics of contents then upsert into database every cron(22 * * * ? *)
'''
import json
from mongo_client import mongo_client, ping_mongodb

def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    print('update content statistics start')

    from modules.update_content_stats.update_content_stats import update_content_stats_main

    # call modules main function
    update_content_stats_main(src_database_name='app-db',
                              src_collection_name='contents',
                              dst_database_name='analytics-db',
                              dst_collection_name='contentStats',
                              contentDateThreshold=30.0, # day unit
                              halfLifeHours=24.0)

    print('update content statistics end')

    return None