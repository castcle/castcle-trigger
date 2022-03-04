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

    print('etl user engagement stats start')

    from modules.etl_user_engagement_stats.etl_user_engagement_stats import etl_user_engagement_stats_main

    # call modules main function
    #! chagen insert many to update one
    etl_user_engagement_stats_main(mongo_client)

    print('etl user engagement stats end')

    return None