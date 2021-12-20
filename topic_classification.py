'''
topic & language classification
function
    obtain language & topics from content then upsert into databases when content is created or updated
'''
import json
from mongo_client import mongo_client, ping_mongodb

def handle(event, context):

    # warming lambda function
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    print(json.dumps(event, indent=4))

    print('topic classification start')

    from modules.topic_classify.topic_classification \
        import topic_classify_main

    # call modules main function
    topic_classify_main(event,   
                        topic_database_name='analytics-db', 
                        topic_collection_name='topics',
                        contents_database_name = 'app-db',
                        contents_collection_name = 'contentinfo')

    print('topic classification end')

    # return output as status code
    return {
        "status": 200
    }
