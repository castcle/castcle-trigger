from datetime import datetime


def fraud_detection_feature_update_main(mongo_client,
                                        source_db: str = "app-db",
                                        source_collection: str = "suspiciouscredentials",
                                        target_db: str = "analytics-db",
                                        target_collection: str = "credentialfeatures",
                                        user_column: str = "seenCredential") -> None:
    result = mongo_client[source_db][source_collection].aggregate([
        {
            '$match': {
                user_column: {
                    '$exists': True
                },
                'firstSeenAt': {
                    '$exists': True
                },
                'lastSeenAt': {
                    '$exists': True
                },
                'verificationStatus': {
                    '$exists': True
                },
                'verifiedAt': {
                    '$exists': True
                }
            }
        }
    ])
    for document in result:
        mongo_client[target_db][target_collection].update_one(
            {
                user_column: document[user_column],
                "firstSeenAt": document["firstSeenAt"],
                "lastSeenAt": document["lastSeenAt"]
            },
            {
                "$set": {
                    "verificationStatus": document["verificationStatus"],
                    "verifiedAt": document["verifiedAt"],
                    "updatedAt": datetime.now()
                }
            },
            upsert=False
        )
        mongo_client[source_db][source_collection].delete_one(
            {
                "_id": document["_id"]
            }
        )
