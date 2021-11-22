# * Topic classification temp
# % This trigger begin monitoring @ 20211112 14:08
import json
from mongo_client import mongo_client, ping_mongodb

# db = mongo_client['analytics-db']


def handle(event, context):

    # warming lambda function
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    from pprint import pprint
    # from pprint import pprint
    # print('the event schema:')
    # pprint(event)
    # print('end of event schema')
    print('topic classification started')

    from modules.topic_classify.topic_classification \
        import topic_classify_main

    # print(json.dumps(event, indent=4)) # comment for now

    # # 1 topic_classify_main
    # topic_classify_main(mongo_client=mongo_client)
    topic_classify_main(event,   
                        topic_database_name='analytics-db', 
                        topic_collection_name='topics',
                        contents_database_name = 'app-db',
                        # contents_collection_name = 'contents')
                        contents_collection_name = 'contentInfo') # changed follows dev's requirement


    return {
        "status": 200
    }
