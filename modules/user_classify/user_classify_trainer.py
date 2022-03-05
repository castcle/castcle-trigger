#License: Castcle
#Author: Atitawat Pol-in
#Version: 1.0.0
import pandas as pd

def query_data_to_df(mongo_client, 
        src_database_name: str='analytics-db', src_collection_name: str='userEngagementStats',
        no_id=True) -> pd.DataFrame:
    f"""
    Get data from 
        src_database_name: {src_database_name}
        src_collection_nae: {src_collection_name}
    """
    # make a query
    query = {}
    cursor = mongo_client[src_database_name][src_collection_name].find(query)

    # import query into pandas
    df = pd.DataFrame(list(cursor))

    # delete _id
    if no_id:
        if '_id' in df.columns:
            del df['_id']

    return df

def user_classify_trainer_main(mongo_client):

    # 1. get data from main collection
    user_engagement_stats_df = query_data_to_df(mongo_client, 
        src_database_name='analytics-db', 
        src_collection_name='userEngagementStats')

    # 2. query analytics-db.topics
    topics_df = query_data_to_df(mongo_client, 
        src_database_name='analytics-db', 
        src_collection_name='topics',
        no_id=False)
    
    


    return user_engagement_stats_df, topics_df