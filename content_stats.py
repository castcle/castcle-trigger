import json
from mongo_client import mongo_client

db = mongo_client['analytics-db']


def handle(event, context):
    from modules.topic_classify.topic_classification \
        import topic_classify_main
    print(json.dumps(event, indent=4))

    # 1 topic_classify_main
    topic_classify_main(mongo_client=mongo_client)
    
    return {
        "status": 200
    }