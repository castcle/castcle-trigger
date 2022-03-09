import pandas as pd
import pickle
import sys
from datetime import datetime, timedelta

from modules.fraud_detection_model_training.models import one_class_classifier_based_on_pca

sys.modules['one_class_classifier_based_on_pca'] = one_class_classifier_based_on_pca


def fraud_detection_prediction_main(mongo_client,
                                    artifact_db: str = "analytics-db",
                                    artifact_collection: str = "frauddetectionmlartifacts",
                                    source_db: str = "analytics-db",
                                    source_collection: str = "credentialfeatures",
                                    target_db: str = "app-db",
                                    target_collection: str = "suspiciouscredentials",
                                    model_name: str = "one-class classifier based on PCA",
                                    user_column: str = "seenCredential",
                                    pred_cooldown_hours: int = 1) -> None:
    ml_artifact = next(
        mongo_client[artifact_db][artifact_collection].aggregate([
            {
                '$match': {
                    'model': model_name
                }
            }, {
                '$sort': {
                    'trainedAt': -1
                }
            }, {
                '$limit': 1
            }
        ])
    )

    features = ml_artifact["features"]
    fields = [user_column, "firstSeenAt", "lastSeenAt"] + features
    result = mongo_client[source_db][source_collection].aggregate([
        {
            '$match': {
                field: {
                    '$exists': True
                } for field in fields
            }
        }, {
            '$sort': {
                user_column: 1,
                'createdAt': 1
            }
        }, {
            '$group': {
                '_id': f'${user_column}',
                'documents': {
                    '$push': '$$ROOT'
                }
            }
        }, {
            '$project': {
                'last': {
                    '$slice': [
                        '$documents', -1
                    ]
                }
            }
        }, {
            '$project': {
                '_id': 0
            }
        }, {
            '$unwind': {
                'path': '$last'
            }
        }, {
            '$replaceRoot': {
                'newRoot': '$last'
            }
        }, {
            '$match': {
                'verificationStatus': {
                    '$exists': False
                },
                'verifiedAt': {
                    '$exists': False
                }
            }
        }
    ])
    df = pd.DataFrame(list(result))
    if not df.empty:
        X = df[features]
        model = pickle.loads(ml_artifact["artifact"])
        y_pred = model.predict(X)
        model_class_dict = {item[0]: item[1] for item in ml_artifact["model_classes"]}
        suspicious_df = df[y_pred.map(model_class_dict) == "bot_class"]
        if not suspicious_df.empty:
            result = mongo_client[source_db][source_collection].aggregate([
                {
                    '$match': {
                        **{
                            field: {
                                '$exists': True
                            } for field in fields
                        },
                        **{
                            'verificationStatus': {
                                '$exists': True
                            },
                            'verifiedAt': {
                                '$exists': True
                            }
                        }
                    }
                }, {
                    '$sort': {
                        user_column: 1,
                        'verifiedAt': 1
                    }
                }, {
                    '$group': {
                        '_id': f'${user_column}',
                        'documents': {
                            '$push': '$$ROOT'
                        }
                    }
                }, {
                    '$project': {
                        'last': {
                            '$slice': [
                                '$documents', -1
                            ]
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0
                    }
                }, {
                    '$unwind': {
                        'path': '$last'
                    }
                }, {
                    '$replaceRoot': {
                        'newRoot': '$last'
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        user_column: 1,
                        'verifiedAt': 1
                    }
                }
            ])
            verified_credential_df = pd.DataFrame(list(result))
            suspicious_df = pd.merge(suspicious_df, verified_credential_df, on=user_column, how="left")
            suspicious_df["time_diff"] = datetime.now() - suspicious_df["verifiedAt"]
            cooldown_period = timedelta(hours=pred_cooldown_hours)
            suspicious_df = suspicious_df[
                (suspicious_df["time_diff"].isnull()) |
                (suspicious_df["time_diff"] >= cooldown_period)
                ][[user_column, "firstSeenAt", "lastSeenAt"]]
            suspicious_df["createdAt"] = datetime.now()
            documents = suspicious_df.to_dict(orient="records")
            for document in documents:
                mongo_client[target_db][target_collection].update_one(
                    {
                        user_column: document[user_column]

                    },
                    {
                        "$setOnInsert": {
                            "firstSeenAt": document["firstSeenAt"],
                            "lastSeenAt": document["lastSeenAt"],
                            'createdAt': document["createdAt"]
                        }
                    },
                    upsert=True
                )
                import time
                time.sleep(0.01)
