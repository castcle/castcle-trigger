import pandas as pd
import pickle
from datetime import datetime, timedelta
from typing import Any, Dict, List


def load_ml_artifact(mongo_client,
                     artifact_db: str = "analytics-db",
                     artifact_collection: str = "frauddetectionmlartifacts",
                     model_name: str = "one-class classifier based on PCA") -> Dict[str, Any]:
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

    return ml_artifact


def load_unverified_data(mongo_client,
                         features: List[str],
                         source_db: str = "analytics-db",
                         source_collection: str = "credentialfeatures",
                         user_column: str = "seenCredential") -> pd.DataFrame:
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

    return pd.DataFrame(list(result))


def predict_suspicious_users(df: pd.DataFrame,
                             ml_artifact: Dict[str, Any],
                             suspicious_class_name: str = "bot_class") -> pd.DataFrame:
    model = pickle.loads(ml_artifact["artifact"])
    y_pred = model.predict(df[ml_artifact["features"]])
    model_class_dict = {item[0]: item[1] for item in ml_artifact["model_classes"]}
    suspicious_df = df[y_pred.map(model_class_dict) == suspicious_class_name]

    return suspicious_df


def load_verified_data(mongo_client,
                       features: List[str],
                       source_db: str = "analytics-db",
                       source_collection: str = "credentialfeatures",
                       user_column: str = "seenCredential") -> pd.DataFrame:
    fields = [user_column, "firstSeenAt", "lastSeenAt"] + features
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

    return pd.DataFrame(list(result))


def select_only_unverified_or_not_on_cooldown_suspicious_users(suspicious_df: pd.DataFrame,
                                                               verified_df: pd.DataFrame,
                                                               user_column: str = "seenCredential",
                                                               pred_cooldown_hours: int = 1) -> pd.DataFrame:
    suspicious_df = pd.merge(suspicious_df, verified_df, on=user_column, how="left")
    suspicious_df["time_diff"] = datetime.now() - suspicious_df["verifiedAt"]
    cooldown_period = timedelta(hours=pred_cooldown_hours)
    suspicious_df = suspicious_df[
        (suspicious_df["time_diff"].isnull()) |
        (suspicious_df["time_diff"] >= cooldown_period)
        ][[user_column, "firstSeenAt", "lastSeenAt"]]
    suspicious_df["createdAt"] = datetime.now()

    return suspicious_df


def save_suspicious_data(mongo_client,
                         df: pd.DataFrame,
                         target_db: str = "app-db",
                         target_collection: str = "suspiciouscredentials",
                         user_column: str = "seenCredential") -> None:
    for document in df.to_dict(orient="records"):
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
    """Collect unverified documents to predict suspicious users"""
    # 1. load a ML artifact
    ml_artifact = load_ml_artifact(
        mongo_client,
        artifact_db=artifact_db,
        artifact_collection=artifact_collection,
        model_name=model_name
    )

    # 2. load unverified data
    unverified_df = load_unverified_data(
        mongo_client,
        ml_artifact["features"],
        source_db=source_db,
        source_collection=source_collection,
        user_column=user_column
    )

    if not unverified_df.empty:
        # 3. predict suspicious users from unverified data
        suspicious_df = predict_suspicious_users(
            unverified_df,
            ml_artifact,
            suspicious_class_name="bot_class"
        )

        if not suspicious_df.empty:
            # 4. load verified data
            verified_df = load_verified_data(
                mongo_client,
                ml_artifact["features"],
                source_db=source_db,
                source_collection=source_collection,
                user_column=user_column
            )

            # 5. select only unverified or not on cooldown suspicious users
            unverified_or_not_on_cooldown_suspicious_df = select_only_unverified_or_not_on_cooldown_suspicious_users(
                suspicious_df,
                verified_df,
                user_column=user_column,
                pred_cooldown_hours=pred_cooldown_hours
            )

            # 6. save suspicious data
            save_suspicious_data(
                mongo_client,
                unverified_or_not_on_cooldown_suspicious_df,
                target_db=target_db,
                target_collection=target_collection,
                user_column=user_column
            )
