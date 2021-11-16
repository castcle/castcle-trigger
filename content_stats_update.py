# This file updates collection 'contentStats' from 'contents'
# just for testing -> in production is running in runtime only (see aggregator_part_topContents)
import json
from mongo_client import mongo_client, ping_mongodb

def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    # print(json.dumps(event, indent=4))
    from modules.update_creator_stats.update_creator_stats import update_creator_stats_main

    update_content_stats_main(src_database_name='app-db',
                              src_collection_name='contents',
                              dst_database_name='analytics-db',
                              dst_collection_name='contentStats',
                              contentDateThreshold=14.0,
                              halfLifeHours=24.0,
                              topContentslimit=100.0)

    print('update content statistics done')

    return None