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
        from modules.fraud_detection_feature_extraction import fraud_detection_feature_extraction_main

        fraud_detection_feature_extraction_main(
            mongo_client,
            source_db="app-db",
            source_collection="feeditems",
            target_db="analytics-db",
            target_collection="credentialfeatures",
            user_column="seenCredential",
            document_limit=500,
            document_threshold=500
        )
    except Exception as e:
        print(f"ERROR: {e}")

    print("fraud detection feature extractor end")
