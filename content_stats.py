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
    
    from modules.utils.download_gcp import download_gcp_main
    from modules.topic_classify.topic_classification \
        import topic_classify_main

    # print(json.dumps(event, indent=4)) # comment for now
    # download gcp.json
    download_gcp_main(local_path='./modules/topic_classify/gcp_data-science_service-account_key.json')
    # # 1 topic_classify_main
    # topic_classify_main(mongo_client=mongo_client)
    topic_classify_main(event,   
                        topic_database_name='analytics-db', 
                        topic_collection_name='topics',
                        contents_database_name = 'app-db',
                        contents_collection_name = 'contents')



    return {
        "status": 200
    }
