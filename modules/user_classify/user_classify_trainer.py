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

def transform_data(user_engagement_stats_df: pd.DataFrame, 
                   topics_df: pd.DataFrame
                   ) -> pd.DataFrame:
    """
    Steps to reproduce the data transformation
    """
    def padding_topicId(user_engagement_stats_df, topics_df) -> pd.DataFrame:
        """
        padding topics id if topicsId are not full
        """
        pad_columns = {'countCommentContentTopic', 'countLikeContentTopic', 
                'countQuoteContentTopic', 'countRecastContentTopic', 
                'countReportContentTopic'}
        # Temp dataframe
        new_user_engagement_stats_df = user_engagement_stats_df[pad_columns]
        # Validation
        columns_name_set = set(user_engagement_stats_df.columns)
        if pad_columns.issubset(columns_name_set):
            for countEngagementTypeTopic in pad_columns:
                new_rows_countEngagementTypeTopic = []
                for row_index, row_userCountEagement in enumerate(user_engagement_stats_df[countEngagementTypeTopic]):
                    print(user_engagement_stats_df[countEngagementTypeTopic].loc[row_index])
                    # case full padding
                    if len(row_userCountEagement) <= 0:
                        full_padding_user_count_engagement = dict.fromkeys(topics_df['_id'], 0)
                    # case partial padding
                    else:
                        full_padding_user_count_engagement = {}
                        existing_topicsId = user_engagement_stats_df\
                            [countEngagementTypeTopic].loc(row_index)\
                                .keys()
                        for _id in topics_df['_id']:
                            if _id not in existing_topicsId:
                                full_padding_user_count_engagement[_id] = 0
                            else:
                                full_padding_user_count_engagement[_id] = \
                                    user_engagement_stats_df[countEngagementTypeTopic]\
                                        [row_userCountEagement][_id]

                    # store new temp array
                    new_rows_countEngagementTypeTopic.append(full_padding_user_count_engagement)
                # when done making new values, assign new values to temp DataFrame
                new_user_engagement_stats_df[pad_columns] = new_rows_countEngagementTypeTopic

            return new_user_engagement_stats_df

        # not Valid Case
        else:
            raise ValueError(f'Padding columns are missing in user_engagement_stats_df, \
                Please check: {pad_columns}')

    transformed_user_engagement_stats_df = padding_topicId(
        user_engagement_stats_df, topics_df)

    return transformed_user_engagement_stats_df

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

    # 3. transform data
    new_user_engagement_stats_df = transform_data(user_engagement_stats_df, topics_df)


    return new_user_engagement_stats_df