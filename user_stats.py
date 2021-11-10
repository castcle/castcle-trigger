# * personalized_content_test
import json
from typing import Collection
from mongo_client import mongo_client, ping_mongodb

# setup databases & collections
analyticsDb = mongo_client['analytics-db']
ml_artifact = 'mlArtifacts_mocked'
content_feature = 'contentFeatures'
#collection = analyticsDb['mlArtifacts_mocked']


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    from modules.personalized_content.personalize_content \
        import personalized_content_main
    from modules.utils.convert_objectid import convert_objectid
    print(json.dumps(event, indent=4))
    print(event)
    #! accountid = account
    user_id = event.get('accountid', None)
    
    user_id = convert_objectid(user_id)

    # db=analyticsDb,collection_name=ml_artifact,content_features=content_feature,user_id=user_id
    personalized_content_result = personalized_content_main(userId=user_id)

    
    return {
        "status": 200
    }
