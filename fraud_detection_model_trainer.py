"""
fraud detection model trainer
function
    train a fraud detection model using the features of verified documents from the credentialfeatures
    and save it to the frauddetectionmlartifacts
    run every minute cron(55 * * * ? *)
"""

from mongo_client import mongo_client, ping_mongodb


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    print("fraud detection model trainer start")

    try:
        from modules.fraud_detection_model_training import fraud_detection_model_training_main

        features = [
            "postReadingTimeAbsSkew",
            "postReadingTimeAbsKurt",
            "postReadingTimeNormStd",
            "postReadingTimeDifferenceNormStd"
        ]
        fraud_detection_model_training_main(
            mongo_client,
            features,
            source_db="analytics-db",
            source_collection="credentialfeatures",
            target_db="analytics-db",
            target_collection="frauddetectionmlartifacts",
            document_limit=10000
        )
    except Exception as e:
        print(f"ERROR: {e}")

    print("fraud detection model trainer end")
