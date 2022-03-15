"""
fraud detection predictor
function
    use a trained model to predict credentials whether they are bot or not,
    and save the suspicious ones to the suspiciouscredentials
    run every minute cron(*/1 * * * ? *)
"""

from mongo_client import mongo_client, ping_mongodb


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    print("fraud detection predictor start")

    try:
        from modules.fraud_detection_prediction import fraud_detection_prediction_main

        fraud_detection_prediction_main(
            mongo_client,
            artifact_db="analytics-db",
            artifact_collection="frauddetectionmlartifacts",
            source_db="analytics-db",
            source_collection="credentialfeatures",
            target_db="app-db",
            target_collection="suspiciouscredentials",
            model_name="one-class classifier based on PCA",
            user_column="seenCredential",
            pred_cooldown_hours=1
        )
    except Exception as e:
        print(f"ERROR: {e}")

    print("fraud detection predictor end")
