'''
personalize content model trainer
function
    train/re-train personal model all user and upsert model artifact to database every cron(25 * * * ? *)
'''
import json
from mongo_client import mongo_client, ping_mongodb

def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    print(json.dumps(event, indent=4))

    print('personalized content training start')

    try:

        from modules.personalized_content.personalize_content_trainer import personalized_content_trainer_main

        # call modules main function
        personalized_content_trainer_main(updatedAtThreshold = 30.0, # define content age
                                        app_db = 'app-db',
                                        engagement_collection = 'engagements',
                                        analytics_db = 'analytics-db',
                                        creator_stats_collection = 'creatorStats',
                                        content_stats_collection = 'contentStats',
                                        dst_database_name = 'analytics-db',
                                        dst_collection_name = 'mlArtifacts',
                                        model_name = 'xgboost')

    except Exception as error:
        print("ERROR", error)

    print('personalized content training end')

    return None
