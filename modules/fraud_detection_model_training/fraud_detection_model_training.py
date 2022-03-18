import pandas as pd
import pickle
from datetime import datetime
from sklearn.metrics import classification_report
from sklearn.model_selection import StratifiedShuffleSplit
from typing import Any, Dict, List, Union

from modules.fraud_detection_model_training.models import OneClassClassifierBasedOnPCA


def load_dataset(mongo_client,
                 features: List,
                 source_db: str = "analytics-db",
                 source_collection: str = "credentialfeatures",
                 document_limit: int = 10000) -> pd.DataFrame:
    """Load dataset for model training"""
    # required fields for model training
    fields = features + ["verificationStatus", "verifiedAt"]
    aggregation_cursor = mongo_client[source_db][source_collection].aggregate([
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
    df = pd.DataFrame(list(aggregation_cursor))

    return df if not df.empty else pd.DataFrame(columns=fields)


def train_model(df: pd.DataFrame, features: List, n_cross_val: int = 5) -> Dict[str, Any]:
    """Train a model for bot prediction using a one-class classification algorithm based on PCA"""
    def evaluate_model(y_true: pd.Series,
                       y_pred: pd.Series,
                       labels: List,
                       target_names: List) -> Dict[str, Union[int, float]]:
        """Evaluate the model's performance with metrics: accuracy, precision, recall, f1-score"""
        # pass ground truth and predicted values to get a model's performance report
        report = classification_report(
            y_true,
            y_pred,
            labels=labels,
            target_names=target_names,
            output_dict=True,
            zero_division=0
        )
        # format the report to a dictionary
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

    # define class number for normal class (bot)
    normal_class_num = 0
    # define class number for anomaly class (human)
    anomaly_class_num = 1
    # define metadata used for converting a predicted class number to a class name
    model_class_dict = {
        normal_class_num: "bot_class",
        anomaly_class_num: "human_class"
    }
    # IDs of documents used in the training process
    doc_ids = df["_id"].to_list()

    # prepare data for the training process
    X = df[features]
    y = df["verificationStatus"].astype(int)

    # train a model N times to evaluate the overall performance using cross validation
    evaluation_report = []
    sss = StratifiedShuffleSplit(n_splits=n_cross_val, train_size=0.8, random_state=0)
    for train_index, test_index in sss.split(X, y):
        X_train, X_test = X.loc[train_index], X.loc[test_index]
        y_train, y_test = y.loc[train_index], y.loc[test_index]
        X_train_bot = X_train[y_train == normal_class_num]
        model = OneClassClassifierBasedOnPCA()
        model.fit(X_train_bot)
        y_pred = model.predict(X_test)
        # save the model's performance for each round
        evaluation_report.append(evaluate_model(
            y_true=y_test,
            y_pred=y_pred,
            labels=list(model_class_dict.keys()),
            target_names=list(model_class_dict.values())
        ))
    # average the values for each evaluation metric as the overall performance
    evaluation_report = pd.DataFrame(evaluation_report).mean().to_dict()

    # train a model for prediction
    X_bot = X[y == normal_class_num]
    model = OneClassClassifierBasedOnPCA()
    model.fit(X_bot)
    # dump the fitted model to a binary object
    pickled_model = pickle.dumps(model)
    # date and time when we got a ready-to-use model
    training_datetime = datetime.now()

    return {
        "model": model.model_name,
        "dataset": doc_ids,
        "features": features,
        "model_classes": [(key, value) for key, value in model_class_dict.items()],
        "artifact": pickled_model,
        "evaluationReport": evaluation_report,
        "trainedAt": training_datetime
    }


def fraud_detection_model_training_main(mongo_client,
                                        features: List,
                                        source_db: str = "analytics-db",
                                        source_collection: str = "credentialfeatures",
                                        target_db: str = "analytics-db",
                                        target_collection: str = "frauddetectionmlartifacts",
                                        document_limit: int = 10000,
                                        n_cross_val: int = 5) -> None:
    """Collect verified documents (dataset) to train a model"""
    # 1. load a dataset
    df = load_dataset(
        mongo_client,
        features,
        source_db=source_db,
        source_collection=source_collection,
        document_limit=document_limit
    )
    print("INFO: dataset for training")
    print(df)

    bot_document_count = len(df[df["verificationStatus"] == False])
    print("INFO: the number of bot documents")
    print(bot_document_count)
    print("INFO: N cross validation")
    print(n_cross_val)
    # case: train a model if the number of bot documents is not less than the n_cross_val
    if bot_document_count >= n_cross_val:
        print("INFO: the number of bot documents is greater than or equal to N cross validation, start training")
        # 2. train a model
        training_result = train_model(df, features, n_cross_val)
        print("INFO: trained model and result from training")
        print(training_result)

        # 3. save the training result, including the machine learning artifact
        mongo_client[target_db][target_collection].insert_one(training_result)
