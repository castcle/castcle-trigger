"""
credential features update
function
    update features of credential then insert into database if this update operation does not match
    every cron(5 * * * * *)
"""

from mongo_client import mongo_client, ping_mongodb


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    print("etl credential features start")

    try:
        from modules.etl_fraud_detection.etl_fraud_detection import etl_fraud_detection_main

        etl_fraud_detection_main(
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
        print(f"[Exception] {e}")

    print("etl credential features end")
