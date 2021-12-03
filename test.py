from app import handle

event = {
    "version": "0",
    "id": "d2fc426c-8814-3a84-7e80-3a18ec260661",
    "detail-type": "MongoDB Database Trigger for app-db.posts",
    "source": "aws.partner/mongodb.com/stitch.trigger/614b4cb6b23f417e5e559c3f",
    "account": "044086777883",
    "time": "2021-09-23T16:30:23Z",
    "region": "us-east-1",
    "resources": [
        "arn:aws:events:us-east-1::event-source/aws.partner/mongodb.com/stitch.trigger/614b4cb6b23f417e5e559c3f"
    ],
    "detail": {
        "_id": {
            "_data": "82614CAB9F000000082B022C0100296E5A10047F0912C604494267AF8C6921C112DCF846645F69640064614CAB95890E884A9264002A0004"
        },
        "operationType": "insert",
        "clusterTime": {
            "T": 1632414623,
            "I": 8
        },
        "fullDocument": {
            "_id": "614cab95890e884a9264002a",
            "title": "title 123"
        },
        "ns": {
            "db": "app-db",
            "coll": "posts"
        },
        "documentKey": {
            "_id": "614cab95890e884a9264002a"
        }
    }
}

# event = 'ERROR'

handle(event, {})
