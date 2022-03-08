import pandas as pd
import pickle
from datetime import datetime
from sklearn.metrics import classification_report
from sklearn.model_selection import StratifiedShuffleSplit
from typing import Dict, List, Union

from modules.fraud_detection_model_training.models import OneClassClassifierBasedOnPCA


def evaluate_model(y_true: pd.Series,
                   y_pred: pd.Series,
                   labels: List,
                   target_names: List) -> Dict[str, Union[int, float]]:

    report = classification_report(
        y_true,
        y_pred,
        labels=labels,
        target_names=target_names,
        output_dict=True,
        zero_division=0
    )
    formatted_report = {}
    for outer_key, outer_value in report.items():
        if isinstance(outer_value, dict):
            for inner_key, inner_value in outer_value.items():
                new_key = f"{outer_key}_{inner_key}".replace(" ", "_")
                formatted_report[new_key] = inner_value
        elif isinstance(outer_value, (int, float)):
            new_key = outer_key.replace(" ", "_")
            formatted_report[new_key] = outer_value

    return formatted_report


def fraud_detection_model_training_main(mongo_client,
                                        features: List,
                                        source_db: str = "analytics-db",
                                        source_collection: str = "credentialfeatures",
                                        target_db: str = "analytics-db",
                                        target_collection: str = "frauddetectionmlartifacts",
                                        document_limit: int = 10000) -> None:

    fields = features + ["verificationStatus", "verifiedAt"]
    result = mongo_client[source_db][source_collection].aggregate([
        {
            '$match': {
                field: {
                    '$exists': True
                } for field in fields
            }
        }, {
            '$sort': {
                'verifiedAt': -1
            }
        }, {
            '$limit': document_limit
        }
    ])
    df = pd.DataFrame(list(result))
    bot_df = df[df["verificationStatus"] == False]
    if len(bot_df) >= 5:
        normal_class_num = 0
        anomaly_class_num = 1
        model_class_dict = {
            normal_class_num: "bot_class",
            anomaly_class_num: "human_class"
        }

        doc_ids = df["_id"].to_list()
        df["isHuman"] = df["verificationStatus"].astype(int)
        X = df[features]
        y = df["isHuman"]
        evaluation_report = []
        sss = StratifiedShuffleSplit(n_splits=5, train_size=0.8, random_state=0)
        for train_index, test_index in sss.split(X, y):
            X_train, X_test = X.loc[train_index], X.loc[test_index]
            y_train, y_test = y.loc[train_index], y.loc[test_index]
            X_train_bot = X_train[y_train == normal_class_num]
            model = OneClassClassifierBasedOnPCA()
            model.fit(X_train_bot)
            y_pred = model.predict(X_test)
            evaluation_report.append(evaluate_model(
                y_true=y_test,
                y_pred=y_pred,
                labels=list(model_class_dict.keys()),
                target_names=list(model_class_dict.values())
            ))
        evaluation_report = pd.DataFrame(evaluation_report).mean().to_dict()

        X_bot = X[y == normal_class_num]
        model = OneClassClassifierBasedOnPCA()
        model.fit(X_bot)
        pickled_model = pickle.dumps(model)
        training_datetime = datetime.now()
        record = {
            "model": model.model_name,
            "dataset": doc_ids,
            "features": features,
            "model_classes": [(key, value) for key, value in model_class_dict.items()],
            "artifact": pickled_model,
            "evaluationReport": evaluation_report,
            "trainedAt": training_datetime
        }
        mongo_client[target_db][target_collection].insert_one(record)
