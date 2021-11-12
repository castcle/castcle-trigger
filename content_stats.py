# * Topic classification temp
import json
from mongo_client import mongo_client, ping_mongodb

# db = mongo_client['analytics-db']


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    # from pprint import pprint
    # print('this is event is:')
    # pprint(event)

    from modules.topic_classify.topic_classification \
        import topic_classify_main

    # print(json.dumps(event, indent=4)) # comment for now

    # # 1 topic_classify_main
    # topic_classify_main(mongo_client=mongo_client)
    topic_classify_main(event) # coupon test running


    return {
        "status": 200
    }
