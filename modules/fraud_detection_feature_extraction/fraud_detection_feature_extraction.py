def fraud_detection_feature_extraction_main(mongo_client,
                                            source_db: str = "app-db",
                                            source_collection: str = "feeditems",
                                            target_db: str = "analytics-db",
                                            target_collection: str = "credentialfeatures",
                                            user_column: str = "seenCredential",
                                            document_limit: int = 500,
                                            document_threshold: int = 500) -> None:
    """Extract features from historical activities of each user"""
    # 1. extract and transform data
    aggregation_cursor = mongo_client[source_db][source_collection].aggregate([
        # select documents which have {user_column}, seenAt, and offScreenAt
        {
            '$match': {
                f'{user_column}': {
                    '$exists': True
                },
                'seenAt': {
                    '$exists': True
                },
                'offScreenAt': {
                    '$exists': True
                }
            }
        },
        # sort documents by {user_column} and seenAt
        {
            '$sort': {
                f'{user_column}': 1,
                'seenAt': 1
            }
        },
        # calculate postReadingTimeSec for each document
        {
            '$addFields': {
                'postReadingTimeSec': {
                    '$divide': [
                        {
                            '$subtract': [
                                '$offScreenAt', '$seenAt'
                            ]
                        }, 1000
                    ]
                }
            }
        },
        # calculate postReadingTimeDifferenceSec for each document
        {
            '$group': {
                '_id': f'${user_column}',
                'documents': {
                    '$push': '$$ROOT'
                }
            }
        }, {
            '$project': {
                'documentsAndPrevTime': {
                    '$zip': {
                        'inputs': [
                            '$documents', {
                                '$concatArrays': [
                                    [
                                        None
                                    ], '$documents.seenAt'
                                ]
                            }
                        ]
                    }
                }
            }
        }, {
            '$unwind': {
                'path': '$documentsAndPrevTime'
            }
        }, {
            '$replaceWith': {
                '$mergeObjects': [
                    {
                        '$arrayElemAt': [
                            '$documentsAndPrevTime', 0
                        ]
                    }, {
                        'prevSeenAt': {
                            '$arrayElemAt': [
                                '$documentsAndPrevTime', 1
                            ]
                        }
                    }
                ]
            }
        }, {
            '$set': {
                'postReadingTimeDifferenceSec': {
                    '$divide': [
                        {
                            '$subtract': [
                                '$seenAt', '$prevSeenAt'
                            ]
                        }, 1000
                    ]
                }
            }
        }, {
            '$unset': 'prevSeenAt'
        },
        # select only documents which have valid postReadingTimeSec (not-negative and not-null) and
        # valid postReadingTimeDifferenceSec (not-null and already not-negative by the sorting before)
        {
            '$match': {
                'postReadingTimeSec': {
                    '$gte': 0
                }
            }
        }, {
            '$match': {
                'postReadingTimeSec': {
                    '$exists': True,
                    '$ne': None
                },
                'postReadingTimeDifferenceSec': {
                    '$exists': True,
                    '$ne': None
                }
            }
        },
        # select only the last N documents for each {user_column} but not below the threshold
        {
            '$group': {
                '_id': f'${user_column}',
                'documents': {
                    '$push': '$$ROOT'
                }
            }
        }, {
            '$project': {
                'lastNDocs': {
                    '$slice': [
                        '$documents', -document_limit
                    ]
                }
            }
        }, {
            '$match': {
                '$expr': {
                    '$gte': [
                        {
                            '$size': '$lastNDocs'
                        }, document_threshold
                    ]
                }
            }
        }, {
            '$unwind': {
                'path': '$lastNDocs'
            }
        }, {
            '$replaceRoot': {
                'newRoot': '$lastNDocs'
            }
        },
        # select columns
        {
            '$project': {
                f'{user_column}': 1,
                'seenAt': 1,
                'offScreenAt': 1,
                'postReadingTimeSec': 1,
                'postReadingTimeDifferenceSec': 1,
                '_id': 0
            }
        },
        # sort documents by {user_column} and postReadingTimeSec
        {
            '$sort': {
                f'{user_column}': 1,
                'postReadingTimeSec': 1
            }
        },
        # calculate firstSeenAt, lastSeenAt, count,
        # postReadingTimeAbsSkew (with 95% of postReadingTime values which are below their 0.95-quantile value),
        # postReadingTimeAbsKurt (with 95% of postReadingTime values which are below their 0.95-quantile value),
        # and postReadingTimeNormStd (with 95% of postReadingTime values which are below their 0.95-quantile value)
        {
            '$group': {
                '_id': f'${user_column}',
                'documents': {
                    '$push': '$$ROOT'
                }
            }
        }, {
            '$project': {
                'documents': 1,
                'postReadingTimeSecArray': '$documents.postReadingTimeSec',
                'position': {
                    '$multiply': [
                        {
                            '$subtract': [
                                {
                                    '$size': '$documents.postReadingTimeSec'
                                }, 1
                            ]
                        }, 0.95
                    ]
                }
            }
        }, {
            '$project': {
                'documents': 1,
                'postReadingTimeSecArray': 1,
                'position': 1,
                'lowerIndex': {
                    '$floor': '$position'
                },
                'upperIndex': {
                    '$ceil': '$position'
                }
            }
        }, {
            '$project': {
                'documents': 1,
                'postReadingTimeSecArray': 1,
                'position': 1,
                'lowerIndex': 1,
                'upperIndex': 1,
                'multiplier': {
                    '$subtract': [
                        '$position', '$lowerIndex'
                    ]
                },
                'lower': {
                    '$arrayElemAt': [
                        '$postReadingTimeSecArray', '$lowerIndex'
                    ]
                },
                'upper': {
                    '$arrayElemAt': [
                        '$postReadingTimeSecArray', '$upperIndex'
                    ]
                }
            }
        }, {
            '$project': {
                'documents': 1,
                'postReadingTimeSecArray': 1,
                'position': 1,
                'lowerIndex': 1,
                'upperIndex': 1,
                'multiplier': 1,
                'lower': 1,
                'upper': 1,
                'quantile95': {
                    '$add': [
                        '$lower', {
                            '$multiply': [
                                {
                                    '$subtract': [
                                        '$upper', '$lower'
                                    ]
                                }, '$multiplier'
                            ]
                        }
                    ]
                }
            }
        }, {
            '$project': {
                'documents': 1,
                'firstSeenAt': {
                    '$min': '$documents.seenAt'
                },
                'lastSeenAt': {
                    '$max': '$documents.seenAt'
                },
                'count': {
                    '$size': '$documents'
                },
                'postReadingTimeSecArray': {
                    '$filter': {
                        'input': '$postReadingTimeSecArray',
                        'as': 'postReadingTimeSec',
                        'cond': {
                            '$lte': [
                                '$$postReadingTimeSec', '$quantile95'
                            ]
                        }
                    }
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'documents': 1,
                'firstSeenAt': 1,
                'lastSeenAt': 1,
                'count': 1,
                'postReadingTimeAbsSkew': {
                    '$abs': {
                        '$multiply': [
                            {
                                '$divide': [
                                    {
                                        '$sqrt': {
                                            '$multiply': [
                                                {
                                                    '$size': '$postReadingTimeSecArray'
                                                }, {
                                                    '$subtract': [
                                                        {
                                                            '$size': '$postReadingTimeSecArray'
                                                        }, 1
                                                    ]
                                                }
                                            ]
                                        }
                                    }, {
                                        '$subtract': [
                                            {
                                                '$size': '$postReadingTimeSecArray'
                                            }, 2
                                        ]
                                    }
                                ]
                            }, {
                                '$divide': [
                                    {
                                        '$divide': [
                                            {
                                                '$reduce': {
                                                    'input': '$postReadingTimeSecArray',
                                                    'initialValue': 0,
                                                    'in': {
                                                        '$sum': [
                                                            '$$value', {
                                                                '$pow': [
                                                                    {
                                                                        '$subtract': [
                                                                            '$$this', {
                                                                                '$avg': '$postReadingTimeSecArray'
                                                                            }
                                                                        ]
                                                                    }, 3
                                                                ]
                                                            }
                                                        ]
                                                    }
                                                }
                                            }, {
                                                '$size': '$postReadingTimeSecArray'
                                            }
                                        ]
                                    }, {
                                        '$pow': [
                                            {
                                                '$divide': [
                                                    {
                                                        '$reduce': {
                                                            'input': '$postReadingTimeSecArray',
                                                            'initialValue': 0,
                                                            'in': {
                                                                '$sum': [
                                                                    '$$value', {
                                                                        '$pow': [
                                                                            {
                                                                                '$subtract': [
                                                                                    '$$this', {
                                                                                        '$avg': '$postReadingTimeSecArray'
                                                                                    }
                                                                                ]
                                                                            }, 2
                                                                        ]
                                                                    }
                                                                ]
                                                            }
                                                        }
                                                    }, {
                                                        '$size': '$postReadingTimeSecArray'
                                                    }
                                                ]
                                            }, 1.5
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                },
                'postReadingTimeAbsKurt': {
                    '$abs': {
                        '$multiply': [
                            {
                                '$divide': [
                                    {
                                        '$divide': [
                                            1, {
                                                '$subtract': [
                                                    {
                                                        '$size': '$postReadingTimeSecArray'
                                                    }, 2
                                                ]
                                            }
                                        ]
                                    }, {
                                        '$subtract': [
                                            {
                                                '$size': '$postReadingTimeSecArray'
                                            }, 3
                                        ]
                                    }
                                ]
                            }, {
                                '$subtract': [
                                    {
                                        '$divide': [
                                            {
                                                '$multiply': [
                                                    {
                                                        '$subtract': [
                                                            {
                                                                '$pow': [
                                                                    {
                                                                        '$size': '$postReadingTimeSecArray'
                                                                    }, 2
                                                                ]
                                                            }, 1
                                                        ]
                                                    }, {
                                                        '$divide': [
                                                            {
                                                                '$reduce': {
                                                                    'input': '$postReadingTimeSecArray',
                                                                    'initialValue': 0,
                                                                    'in': {
                                                                        '$sum': [
                                                                            '$$value', {
                                                                                '$pow': [
                                                                                    {
                                                                                        '$subtract': [
                                                                                            '$$this', {
                                                                                                '$avg': '$postReadingTimeSecArray'
                                                                                            }
                                                                                        ]
                                                                                    }, 4
                                                                                ]
                                                                            }
                                                                        ]
                                                                    }
                                                                }
                                                            }, {
                                                                '$size': '$postReadingTimeSecArray'
                                                            }
                                                        ]
                                                    }
                                                ]
                                            }, {
                                                '$pow': [
                                                    {
                                                        '$divide': [
                                                            {
                                                                '$reduce': {
                                                                    'input': '$postReadingTimeSecArray',
                                                                    'initialValue': 0,
                                                                    'in': {
                                                                        '$sum': [
                                                                            '$$value', {
                                                                                '$pow': [
                                                                                    {
                                                                                        '$subtract': [
                                                                                            '$$this', {
                                                                                                '$avg': '$postReadingTimeSecArray'
                                                                                            }
                                                                                        ]
                                                                                    }, 2
                                                                                ]
                                                                            }
                                                                        ]
                                                                    }
                                                                }
                                                            }, {
                                                                '$size': '$postReadingTimeSecArray'
                                                            }
                                                        ]
                                                    }, 2
                                                ]
                                            }
                                        ]
                                    }, {
                                        '$multiply': [
                                            3, {
                                                '$pow': [
                                                    {
                                                        '$subtract': [
                                                            {
                                                                '$size': '$postReadingTimeSecArray'
                                                            }, 1
                                                        ]
                                                    }, 2
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                },
                'postReadingTimeNormStd': {
                    '$divide': [
                        {
                            '$stdDevSamp': '$postReadingTimeSecArray'
                        }, {
                            '$avg': '$postReadingTimeSecArray'
                        }
                    ]
                }
            }
        }, {
            '$unwind': {
                'path': '$documents'
            }
        }, {
            '$replaceWith': {
                '$mergeObjects': [
                    '$documents', {
                        'firstSeenAt': '$firstSeenAt'
                    }, {
                        'lastSeenAt': '$lastSeenAt'
                    }, {
                        'count': '$count'
                    }, {
                        'postReadingTimeAbsSkew': '$postReadingTimeAbsSkew'
                    }, {
                        'postReadingTimeAbsKurt': '$postReadingTimeAbsKurt'
                    }, {
                        'postReadingTimeNormStd': '$postReadingTimeNormStd'
                    }
                ]
            }
        },
        # sort documents by {user_column} and postReadingTimeDifferenceSec
        {
            '$sort': {
                f'{user_column}': 1,
                'postReadingTimeDifferenceSec': 1
            }
        },
        # calculate createdAt, updatedAt,
        # and postReadingTimeDifferenceNormStd (with 95% of postReadingTimeDifference values
        # which are below their 0.95-quantile value)
        {
            '$group': {
                '_id': f'${user_column}',
                'documents': {
                    '$push': '$$ROOT'
                }
            }
        }, {
            '$project': {
                'documents': 1,
                f'{user_column}': {
                    '$arrayElemAt': [
                        f'$documents.{user_column}', 0
                    ]
                },
                'firstSeenAt': {
                    '$arrayElemAt': [
                        '$documents.firstSeenAt', 0
                    ]
                },
                'lastSeenAt': {
                    '$arrayElemAt': [
                        '$documents.lastSeenAt', 0
                    ]
                },
                'count': {
                    '$arrayElemAt': [
                        '$documents.count', 0
                    ]
                },
                'postReadingTimeAbsSkew': {
                    '$arrayElemAt': [
                        '$documents.postReadingTimeAbsSkew', 0
                    ]
                },
                'postReadingTimeAbsKurt': {
                    '$arrayElemAt': [
                        '$documents.postReadingTimeAbsKurt', 0
                    ]
                },
                'postReadingTimeNormStd': {
                    '$arrayElemAt': [
                        '$documents.postReadingTimeNormStd', 0
                    ]
                }
            }
        }, {
            '$project': {
                'documents': 1,
                f'{user_column}': 1,
                'firstSeenAt': 1,
                'lastSeenAt': 1,
                'count': 1,
                'postReadingTimeAbsSkew': 1,
                'postReadingTimeAbsKurt': 1,
                'postReadingTimeNormStd': 1,
                'postReadingTimeDifferenceSecArray': '$documents.postReadingTimeDifferenceSec',
                'position': {
                    '$multiply': [
                        {
                            '$subtract': [
                                {
                                    '$size': '$documents.postReadingTimeDifferenceSec'
                                }, 1
                            ]
                        }, 0.95
                    ]
                }
            }
        }, {
            '$project': {
                'documents': 1,
                f'{user_column}': 1,
                'firstSeenAt': 1,
                'lastSeenAt': 1,
                'count': 1,
                'postReadingTimeAbsSkew': 1,
                'postReadingTimeAbsKurt': 1,
                'postReadingTimeNormStd': 1,
                'postReadingTimeDifferenceSecArray': 1,
                'position': 1,
                'lowerIndex': {
                    '$floor': '$position'
                },
                'upperIndex': {
                    '$ceil': '$position'
                }
            }
        }, {
            '$project': {
                'documents': 1,
                f'{user_column}': 1,
                'firstSeenAt': 1,
                'lastSeenAt': 1,
                'count': 1,
                'postReadingTimeAbsSkew': 1,
                'postReadingTimeAbsKurt': 1,
                'postReadingTimeNormStd': 1,
                'postReadingTimeDifferenceSecArray': 1,
                'position': 1,
                'lowerIndex': 1,
                'upperIndex': 1,
                'multiplier': {
                    '$subtract': [
                        '$position', '$lowerIndex'
                    ]
                },
                'lower': {
                    '$arrayElemAt': [
                        '$postReadingTimeDifferenceSecArray', '$lowerIndex'
                    ]
                },
                'upper': {
                    '$arrayElemAt': [
                        '$postReadingTimeDifferenceSecArray', '$upperIndex'
                    ]
                }
            }
        }, {
            '$project': {
                'documents': 1,
                f'{user_column}': 1,
                'firstSeenAt': 1,
                'lastSeenAt': 1,
                'count': 1,
                'postReadingTimeAbsSkew': 1,
                'postReadingTimeAbsKurt': 1,
                'postReadingTimeNormStd': 1,
                'postReadingTimeDifferenceSecArray': 1,
                'position': 1,
                'lowerIndex': 1,
                'upperIndex': 1,
                'multiplier': 1,
                'lower': 1,
                'upper': 1,
                'quantile95': {
                    '$add': [
                        '$lower', {
                            '$multiply': [
                                {
                                    '$subtract': [
                                        '$upper', '$lower'
                                    ]
                                }, '$multiplier'
                            ]
                        }
                    ]
                }
            }
        }, {
            '$project': {
                'documents': 1,
                f'{user_column}': 1,
                'firstSeenAt': 1,
                'lastSeenAt': 1,
                'count': 1,
                'postReadingTimeAbsSkew': 1,
                'postReadingTimeAbsKurt': 1,
                'postReadingTimeNormStd': 1,
                'postReadingTimeDifferenceSecArray': {
                    '$filter': {
                        'input': '$postReadingTimeDifferenceSecArray',
                        'as': 'postReadingTimeDifferenceSec',
                        'cond': {
                            '$lte': [
                                '$$postReadingTimeDifferenceSec', '$quantile95'
                            ]
                        }
                    }
                }
            }
        }, {
            '$project': {
                '_id': 0,
                f'{user_column}': 1,
                'firstSeenAt': 1,
                'lastSeenAt': 1,
                'count': 1,
                'postReadingTimeAbsSkew': 1,
                'postReadingTimeAbsKurt': 1,
                'postReadingTimeNormStd': 1,
                'postReadingTimeDifferenceNormStd': {
                    '$divide': [
                        {
                            '$stdDevSamp': '$postReadingTimeDifferenceSecArray'
                        }, {
                            '$avg': '$postReadingTimeDifferenceSecArray'
                        }
                    ]
                },
                'createdAt': '$$NOW',
                'updatedAt': '$$NOW'
            }
        },
        # select documents which have {user_column}, firstSeenAt, lastSeenAt, count, postReadingTimeAbsSkew,
        # postReadingTimeAbsKurt, postReadingTimeNormStd, postReadingTimeDifferenceNormStd, createdAt, and updatedAt,
        # and they are not-null
        {
            '$match': {
                f'{user_column}': {
                    '$exists': True,
                    '$ne': None
                },
                'firstSeenAt': {
                    '$exists': True,
                    '$ne': None
                },
                'lastSeenAt': {
                    '$exists': True,
                    '$ne': None
                },
                'count': {
                    '$exists': True,
                    '$ne': None
                },
                'postReadingTimeAbsSkew': {
                    '$exists': True,
                    '$ne': None
                },
                'postReadingTimeAbsKurt': {
                    '$exists': True,
                    '$ne': None
                },
                'postReadingTimeNormStd': {
                    '$exists': True,
                    '$ne': None
                },
                'postReadingTimeDifferenceNormStd': {
                    '$exists': True,
                    '$ne': None
                },
                'createdAt': {
                    '$exists': True,
                    '$ne': None
                },
                'updatedAt': {
                    '$exists': True,
                    '$ne': None
                }
            }
        },
        # sort documents by {user_column}
        {
            '$sort': {
                f'{user_column}': 1
            }
        }
    ], allowDiskUse=True)
    documents = list(aggregation_cursor)
    print(f"INFO: the number of documents is {len(documents)}")
    print(f"INFO: extracted features are {documents}")

    # 2. load data
    for document in documents:
        # insert documents dynamically by {user_column}, firstSeenAt, lastSeenAt
        mongo_client[target_db][target_collection].update_one(
            {
                user_column: document[user_column],
                "firstSeenAt": document["firstSeenAt"],
                "lastSeenAt": document["lastSeenAt"]
            },
            {
                "$setOnInsert": {
                    "count": document["count"],
                    "postReadingTimeAbsSkew": document["postReadingTimeAbsSkew"],
                    "postReadingTimeAbsKurt": document["postReadingTimeAbsKurt"],
                    "postReadingTimeNormStd": document["postReadingTimeNormStd"],
                    "postReadingTimeDifferenceNormStd": document["postReadingTimeDifferenceNormStd"],
                    'createdAt': document["createdAt"],
                    'updatedAt': document["updatedAt"]
                }
            },
            upsert=True
        )
