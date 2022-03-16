import json
from mongo_client import mongo_client, ping_mongodb

def handle(event, context):
    if event.get("source") == "serverless-plugin-warmup":
        ping_mongodb()
        print("WarmUp - Lambda is warm!")
        return

    print('feed performance calculator start')

    from modules.feed_performance_monitor.feed_performance_monitor \
        import feed_performance_monitor_main

    # call modules main function
    feed_performance_monitor_main(mongo_client)

    print('feed performance calculator end')

    return None