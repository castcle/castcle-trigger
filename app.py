# import json
# from mongo_client import mongo_client

# db = mongo_client['analytics-db']


def handle(event, context):
    print('MONGODB TRIGGERS')

    # if event == 'ERROR':
    #     print('ERROR')
    #     return

    # print(json.dumps(event, indent=4))

    # title = event['detail']['fullDocument']['title']
    # post_id = db.posts.insert_one({'title': title}).inserted_id

    # print(f'post id = {post_id}')
