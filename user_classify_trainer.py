'''
Scheduling:
hourly
'''
import json
from mongo_client import mongo_client, ping_mongodb
import datetime


def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    from modules.user_classify.user_classify_trainer_pred import user_classify_trainer_main
    
    print(json.dumps(event, indent=4))
    print(event)

    try:
        # call modules main function
        user_classify_trainer_main_result = user_classify_trainer_main(mongo_client)
    except Exception as e:
        print(f'[Exception]: {e}')

    print('[INFO] Update model user classify successfully')

    # return output as status code & timestamp
    return {
        "message": "User classify trained",
        "trained_at": str(datetime.datetime.now())
    }
