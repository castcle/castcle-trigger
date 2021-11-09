# * Topic classification temp
import json
from mongo_client import mongo_client

db = mongo_client['analytics-db']


def handle(event, context):

    from pprint import pprint
    pprint(event)

    

    if event.get("source") == "serverless-plugin-warmup":
        print("WarmUp - Lambda is warm!")
        return

    from modules.topic_classify.topic_classification \
        import topic_classify_main
    print(json.dumps(event, indent=4))

    # 1 topic_classify_main
    topic_classify_main(mongo_client=mongo_client)

    return {
        "status": 200
    }
