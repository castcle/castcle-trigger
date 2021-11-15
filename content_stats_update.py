# This file updates collection 'contentStats' from 'contents'
# just for testing -> in production is running in runtime only (see aggregator_part_topContents)
import json
import sys
from mongo_client import mongo_client, ping_mongodb
from bson.objectid import ObjectId
from bson import regex
from datetime import datetime, timedelta

def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    # print(json.dumps(event, indent=4))
    from modules.update_creator_stats.update_creator_stats import update_creator_stats_main

    # define content parameters
    src_database_name = 'app-db',
    src_collection_name = 'contents'
    contentDateThreshold = 14
    halfLifeHours = 24
    topContentslimit = 100

    update_content_stats_main(src_database_name=src_database_name,
                              src_collection_name=src_collection_name,
                              contentDateThreshold=contentDateThreshold,
                              halfLifeHours=halfLifeHours,
                              topContentslimit=topContentslimit)

    print('update content statistics done')

    return None