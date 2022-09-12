"""
suggest services default content for user have no relationship
function
    calculate popularity model for content for each country then insert data to default guest db and will be used for prediction
    run every 1 day cron(0 0 * * *)
"""

from mongo_client import mongo_client, ping_mongodb


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    print("suggest services default content start")

    try:
        from modules.suggest_services.suggest_services_member_content import suggest_content_member_main

        suggest_content_member_main(
            mongo_client
        )
    except Exception as e:
        print(f"ERROR: {e}")

    print("suggest services default content end")
