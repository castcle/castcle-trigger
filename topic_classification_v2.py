"""
topic classification version2
This model will convert word/sentence to embedding vector, Compare between interedted sentence with dot-product vector in dataset.
function
    Query data from analytics-db.contentfiltering -> predict topic each content.
    Update to analytics-db.contentfiltering
    run every 12hr cron(0 */12 * * *)
#ref(th): https://github.com/spps-supalerk/CategorizationUsingUniversalSentenceEncoder/blob/main/CategorizationUsingUniversalSentenceEncoder.ipynb
"""

from mongo_client import mongo_client as client
from mongo_client import ping_mongodb


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    print("topic classification version2 start")

    try:
        from modules.topic_classification_v2.topic_classification_v2 import topic_classify_main
        topic_classify_main(
            client,
            target_db="analytics-db",
            target_collection="contentfiltering"
        )
    except Exception as e:
        print(f"ERROR: {e}")

    print("fraud detection predictor end")
