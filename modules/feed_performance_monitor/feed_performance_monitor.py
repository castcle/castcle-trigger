def get_feed_data(mongo_client):
    
    def query_content_seen_engaged(mongo_client):
        cursor = mongo_client['app-db']['engagements'].aggregate([
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
                '$lookup': {
                    'from': 'users', 
                    'localField': 'user', 
                    'foreignField': '_id', 
                    'as': '_users'
                }
            }, {
                '$unwind': {
                    'path': '$_users', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$project': {
                    '_id': 1, 
                    'userId': 1, 
                    'type': 1, 
                    'contentId': 1, 
                    'updatedAt': 1, 
                    'itemId': 1, 
                    'ownerAccount': '$_users.ownerAccount'
                }
            }
        ])

        import pandas as pd
        df =  pd.DataFrame(list(cursor))
        df = df[['_id', 'type', 'contentId', 'updatedAt', 'itemId', 'ownerAccount']]
        df['_id'] = df['_id'].astype('str')
        df['contentId'] = df['contentId'].astype('str')
        df['ownerAccount'] = df['ownerAccount'].astype('str')

        return df
    
    def query_feeditems(mongo_client):
        cursor = mongo_client['app-db']['feeditems'].aggregate([
            {
                '$match': {
                    'analytics': {
                        '$exists': True
                    }
                }
            }, {
                '$project': {
                    'content': 1, 
                    'viewer': 1, 
                    'score': '$analytics.score', 
                    'source': '$analytics.source', 
                    'createdAt': 1, 
                    'seenCredential': 1, 
                    'seenAt': 1
                }
            }
        ])

        import pandas as pd
        df =  pd.DataFrame(list(cursor))

        df['_id'] = df['_id'].astype('str')
        df['viewer'] = df['viewer'].astype('str')
        df['content'] = df['content'].astype('str')

        df.rename(columns={
            'content': 'contentId',
            'viewer': 'ownerAccount'
        }, inplace=True)

        return df
    
    import pandas as pd
    feeditems = query_feeditems(mongo_client)
    content_seen_engaged = query_content_seen_engaged(mongo_client)
    joined_df = pd.merge(feeditems, content_seen_engaged, how='left', on=['contentId', 'ownerAccount'])
    joined_df = joined_df[joined_df['score'] > 0]
    
    return joined_df

def _calculate(joined_df):
    from datetime import datetime
    # engagedSuggest
    overall_engaged_count = joined_df.query('type in ("like", "comment", "recast", "quote")').shape[0]
    personal_engaged_count = joined_df.query('type in ("like", "comment", "recast", "quote") & source == "personal"').shape[0]
    global_engaged_count = joined_df.query('type in ("like", "comment", "recast", "quote") & source == "global"').shape[0]
    overall_count = joined_df.shape[0]
    personal_suggested_count = joined_df.query('source == "personal"').shape[0]
    global_suggested_count = joined_df.query('source == "global"').shape[0]
    
    perC_engaged_score_percent = (personal_engaged_count/personal_suggested_count) * 100
    coldS_engaged_score_percent = (global_engaged_count/global_suggested_count) * 100
    overall_engaged_score_percent = (overall_engaged_count/overall_count) * 100
    
    print(f'Personalize content feed suggestions engaged percent: {perC_engaged_score_percent}')
    print(f'Coldstart feed suggestions engaged percent: {coldS_engaged_score_percent}')
    print(f'Overall feed suggestions engaged percent: {overall_engaged_score_percent}')
    
    return {
        "message": "Calculated",
        "result": {
            "personalize_content_feed_score_in_percent": round(perC_engaged_score_percent, 2),
            "coldstart_feed_score_in_percent": round(coldS_engaged_score_percent, 2),
            "overall_feed_score_in_percent": round(overall_engaged_score_percent, 2),
            "calculatedAt": datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        }
    }

def _calculate_save(mongo_client, result):
    
    try:
        mongo_client['analytics-db'].create_collection('feedperformances')
    except:
        pass
    
    mongo_client['analytics-db']['feedperformances'].insert_one(result['result'])
    
    return None

def feed_performance_monitor_main(mongo_client):
    from datetime import datetime
    
    joined_df = get_feed_data(mongo_client)
    
    result = _calculate(joined_df)
    
    _calculate_save(mongo_client, result)

    print(f'now: {datetime.now()}')
    
    return result