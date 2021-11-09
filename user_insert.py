# * coldstart_prediction
import json
from mongo_client import mongo_client, ping_mongodb

db = mongo_client['analytics-db']


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    from modules.coldstart_prediction.coldstart \
        import coldstart_main
    print(json.dumps(event, indent=4))
    print(event)

    countryId = event.get('countryId', None)

    coldstart_main_result = coldstart_main(countryId=countryId)

    #! debug
    print(coldstart_main_result)

    return {
        "status": 200,
        "country": countryId,
        "contents": coldstart_main_result
    }
