"""
fraud detection feature extractor
function
    extract features from historical activities of each credential then insert into the credentialfeatures
    and will be used for prediction
    run every 5 minutes cron(*/5 * * * ? *)
"""

from mongo_client import mongo_client, ping_mongodb


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    print("fraud detection feature extractor start")

    try:
        from modules.suggest_services_default_content import suggest_services_default_content_main

        suggest_services_default_content_main(
            mongo_client
        )
    except Exception as e:
        print(f"ERROR: {e}")

    print("fraud detection feature extractor end")
