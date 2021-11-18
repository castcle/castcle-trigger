import json
from typing import Collection
from mongo_client import mongo_client, ping_mongodb

def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    
    print(json.dumps(event, indent=4))
    print(event)
    
    return {
        "status": 200
    }
