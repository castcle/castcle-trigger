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
    def padding_countTypeTopicId(user_engagement_stats_df, topics_df) -> pd.DataFrame:
        """
        padding topics id if topicsId are not full
        """
        # fix columns name
        pad_columns = {'countCommentContentTopic', 'countLikeContentTopic', 
                'countQuoteContentTopic', 'countRecastContentTopic', 
                'countReportContentTopic'}
        full_topic_id_dict = topics_df['_id']
        
        # Temp dataframe
        new_user_engagement_stats_df = user_engagement_stats_df.copy()
        # Validation
        columns_name_set = set(user_engagement_stats_df.columns)
        if pad_columns.issubset(columns_name_set): 
            for index, row in user_engagement_stats_df.iterrows():
                userId = row['userId']
                countCommentContentTopic, countCommentContentTopic_head = row['countCommentContentTopic'], 'countCommentContentTopic' 
                countLikeContentTopic, countLikeContentTopic_head = row['countLikeContentTopic'], 'countLikeContentTopic' 
                countQuoteContentTopic, countQuoteContentTopic_head = row['countQuoteContentTopic'], 'countQuoteContentTopic' 
                countRecastContentTopic, countRecastContentTopic_head = row['countRecastContentTopic'], 'countRecastContentTopic' 
                countReportContentTopic, countReportContentTopic_head = row['countReportContentTopic'], 'countReportContentTopic' 

                changing_columns_by_row = (
                    (countCommentContentTopic, countCommentContentTopic_head), 
                    (countLikeContentTopic, countLikeContentTopic_head),
                    (countQuoteContentTopic, countQuoteContentTopic_head), 
                    (countRecastContentTopic, countRecastContentTopic_head), 
                    (countReportContentTopic, countReportContentTopic_head)
                    )
                
                #! temp solution
                for changing_column_by_row, header in changing_columns_by_row:
                    if len(changing_column_by_row) <= 0:
                        full_padding_user_count_engagement_by_row = dict.fromkeys(full_topic_id_dict, 0)
                    else:
                        full_padding_user_count_engagement_by_row = dict.fromkeys(full_topic_id_dict, 0)
                        full_padding_user_count_engagement_by_row.update(changing_column_by_row)
                    
                    user_engagement_stats_df.at[index, header] = full_padding_user_count_engagement_by_row

            return user_engagement_stats_df

        # not Valid Case
        else:
            raise ValueError(f'Padding columns are missing in user_engagement_stats_df, \
                Please check: {pad_columns}')

    transformed_user_engagement_stats_df = padding_countTypeTopicId(
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