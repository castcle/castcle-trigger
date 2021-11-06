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
        import personalized_content
    print(json.dumps(event, indent=4))
    
    user_id = event['accountid']
    
    personalized_content_result = personalized_content(analyticsDb, 
                                                       ml_artifact,
                                                       content_feature,
                                                       user_id
	)
    
    
    return {
        "status": 200
    }