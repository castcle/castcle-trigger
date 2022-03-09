"""
fraud detection feature updater
function
    update verification statuses of the verified suspicious credentials to their features in the credentialfeatures
    for further model retraining
    run if any document in the suspiciouscredentials is verified by the application
"""

from mongo_client import mongo_client, ping_mongodb


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    print("fraud detection feature updater start")

    try:
        from modules.fraud_detection_feature_update import fraud_detection_feature_update_main

        fraud_detection_feature_update_main(
            mongo_client,
            source_db="app-db",
            source_collection="suspiciouscredentials",
            target_db="analytics-db",
            target_collection="credentialfeatures",
            user_column="seenCredential"
        )
    except Exception as e:
        print(f"ERROR: {e}")

    print("fraud detection feature updater end")
