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
    else:
        df['_id'] = df['_id'].astype('str')

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

def preprocessing(df: pd.DataFrame):
    """
    Preprocessing DataFrame before inject into ML Modeling
    """
    def flatten_json_like_data(df: pd.DataFrame, source_col: str) -> pd.DataFrame:
        """
        Flattening values in JSON format to DataFrame columns
        """
        new_df = df.copy()
        A = pd.json_normalize(df[source_col]).add_prefix(source_col+'.')
        new_df = pd.concat([new_df, A], axis=1)
        new_df.drop(columns=[source_col], axis=1, inplace=True)

        # fill na
        new_df.fillna(0)
        
        return new_df

    preprocessing_columns = {
        'countCommentContentTopic', 'countLikeContentTopic', 
        'countQuoteContentTopic', 'countRecastContentTopic', 
        'countReportContentTopic'
    }

    # pre dropping column
    df = df.drop(['timestamp'], axis=1, errors='ignore')

    for column in preprocessing_columns:
        df = flatten_json_like_data(df, column)

    # drop userId column
    df = df.drop(['userId'], axis=1, errors='ignore')

    return df

def trainning_model(ready_df: pd.DataFrame, **kwargs):
    """
    Training Model : 
        K-Means Clustering
    """
    model_name = kwargs.get('model_name', None).lower()

    if model_name is None:
        raise ValueError("model_name not specified")
    
    if model_name in ['kmeans', 'kmean']:
        # fix cannot call module
#        import sys
#        sys.path.append(".")
        
        # checking
        n_clusters = kwargs.get('n_clusters', 7)

        from .kmeans import KmeansClusteringModel
        model = KmeansClusteringModel(ready_df, n_clusters=n_clusters)
        model.fit(ready_df)

        return model

def saving_model(mongo_client, model, model_name,
    db_nm: str='analytics-db', coll_nm: str='mlArtifacts_userClassify'):
    """
    Saving model object with pickle to MongoDB
        by userId
    """
    import pickle
    from datetime import datetime
    pickle_model = pickle.dumps(model)

    collection_obj = mongo_client[db_nm][coll_nm]

    document = collection_obj.update_one(
        {
            'model': model_name
        }, {
            '$set': {
                'model': model_name,
                'artifact': pickle_model,
                'trainedAt': datetime.now()
            }
        }, upsert= True
    )

    print('[INFO] Saving model successfully')

    return None

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

    # 4. Preprocessing
    preprocessed_df = preprocessing(new_user_engagement_stats_df)

    # 5. Training Model
    model_config = {
        'model_name': 'kmeans',
        'n_clusters': 10
    }
    model = trainning_model(preprocessed_df, 
            model_name=model_config['model_name'], 
            n_clusters=model_config['n_clusters'])

    # 6. Save model artifact to mongodb
    saving_model(mongo_client=mongo_client, model=model, 
        model_name=model_config['model_name'])

    return {
        "message": "Success"
    }