# Author: Atitawat Pol-in
# Version: 1.0.0

def extract_data(mongo_client, 
        src_db_name: str='app-db', src_col_name: str='engagements'):
    f"""
    Extract data from collection name: {src_col_name}
    """

    pipeline = [
        {
            '$match': {
                'targetRef.$ref': 'content'
            }
        }, {
            '$set': {
                'contentId': '$targetRef.$id', 
                'userId': '$user'
            }
        }, {
            '$project': {
                '_id': 1, 
                'userId': 1, 
                'type': 1, 
                'contentId': 1, 
                'visibility': 1, 
                'createdAt': 1, 
                'updatedAt': 1
            }
        }, {
            '$lookup': {
                'from': 'contentinfo', 
                'pipeline': [
                    {
                        '$match': {
                            'topics': {
                                '$exists': True
                            }
                        }
                    }
                ], 
                'localField': 'contentId', 
                'foreignField': 'contentId', 
                'as': 'contentinfo'
            }
        }, {
            '$unwind': {
                'path': '$contentinfo', 
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$set': {
                'topics': '$contentinfo.topics'
            }
        }, {
            '$project': {
                '_id': 0, 
                'visibility': 0, 
                'createdAt': 0, 
                'contentinfo': 0, 
                'updatedAt': 0
            }
        }, {
            '$unwind': {
                'path': '$topics', 
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$group': {
                '_id': {
                    'userId': '$userId', 
                    'type': '$type', 
                    'topicId': '$topics'
                }, 
                'countTopicId': {
                    '$count': {}
                }
            }
        }, {
            '$sort': {
                '_id.userId': 1, 
                '_id.type': 1
            }
        }
    ]
    mongo_cursor = mongo_client[src_db_name][src_col_name].aggregate(pipeline)

    

    return mongo_cursor

def restructure_userEngagementStats(mongo_cursor):
    """
    Change Structure of userEngagementStats
    !to fix:
        2 loops
    """
    import datetime
    
    pre_result = {}
    type_mapper = {
        'comment': 'countCommentContentTopic',
        'like': 'countLikeContentTopic',
        'quote': 'countQuoteContentTopic',
        'recast': 'countRecastContentTopic',
        'report': 'countReportContentTopic'
    }
    
    for document in mongo_cursor:
        userId = document['_id']['userId']
        engagement_type = document['_id']['type']
        ds_topicId = str(document['_id']['topicId'])
        count_topicId = document['countTopicId']
        
        if userId not in pre_result:
            
            # initial empty dict
            pre_result[userId] = {
                'countCommentContentTopic': {},
                'countLikeContentTopic': {},
                'countQuoteContentTopic': {},
                'countRecastContentTopic': {},
                'countReportContentTopic': {}
            }
            
            if ds_topicId not in pre_result[userId][type_mapper[engagement_type]]:
                pre_result[userId][type_mapper[engagement_type]][ds_topicId] = count_topicId
                
        else:
            pre_result[userId][type_mapper[engagement_type]][ds_topicId] = count_topicId

    user_engagement_stats_arr = []

    for __userId, __count_engagement_type in pre_result.items():
        _tmp_result = {
            'userId': __userId,
            'countCommentContentTopic':  __count_engagement_type['countCommentContentTopic'],
            'countLikeContentTopic':  __count_engagement_type['countLikeContentTopic'],
            'countQuoteContentTopic':  __count_engagement_type['countQuoteContentTopic'],
            'countRecastContentTopic':  __count_engagement_type['countRecastContentTopic'],
            'countReportContentTopic':  __count_engagement_type['countReportContentTopic'],
            'timestamp': datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        }

        user_engagement_stats_arr.append(_tmp_result)
    
    return user_engagement_stats_arr

def loading(mongo_client, array_of_document: list,
        trgt_db_name: str='analytics-db', trgt_collection_name: str='userEngagementStats'):
    f"""
    FullLoad data into mongoDB
    target database name: {trgt_db_name}
    target collection name: {trgt_collection_name}
    """

    # 1. delete all documents
    mongo_client[trgt_db_name][trgt_collection_name].delete_many(
        {}
    )

    # 2. insert all documents
    mongo_client[trgt_db_name][trgt_collection_name].insert_many(array_of_document)
    print('[INFO] Load to mongodb successfully')

    return 

def etl_user_engagement_stats_main(mongo_client):

    # 1. Extract data from source
    mongo_cursor = extract_data(mongo_client)

    # 2. restructure of extraction result
    user_engagement_result = restructure_userEngagementStats(mongo_cursor)
    
    # 3. load 
    loading(mongo_client, user_engagement_result)

    return