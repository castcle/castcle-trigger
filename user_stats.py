	#* personalized_content_test
import json
from typing import Collection
from mongo_client import mongo_client

# setup databases & collections
analyticsDb = mongo_client['analytics-db']
ml_artifact = 'mlArtifacts_mocked'
content_feature = 'contentFeatures'
#collection = analyticsDb['mlArtifacts_mocked']

def handle(event, context):
    from modules.personalized_content.personalize_content \
        import personalized_content_main
    print(json.dumps(event, indent=4))
    print(event)
    #! accountid = account
    user_id = event.get('accountid', None)
    
    #db=analyticsDb,collection_name=ml_artifact,content_features=content_feature,user_id=user_id
    personalized_content_result = personalized_content_main()
    
    #! debug
    #print(personalized_content_result)
    
    return {
        "status": 200
    }