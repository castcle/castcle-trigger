'''
Coldstart Content Trainer
Main Logic
1. Train ML artifact

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

    from modules.coldstart.coldstart_trainer import coldstart_train_main
    
    print(json.dumps(event, indent=4))
    print(event)

    # call modules main function
    coldstart_main_result = coldstart_train_main(
                                mongo_client,
                                updatedAtThreshold=30.0
                                )

    print('update content statistics start')

    # return output as status code & timestamp
    return {
        "status": 200,
        "trained_at": str(datetime.datetime.now())
    }
