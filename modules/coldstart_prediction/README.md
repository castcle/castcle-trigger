# Coldstart trainer
content recommendation for anonymous or non-active user by country
## scenario
### image
run this trainer every 30 minute (subject to change)
1. prepare aggregated features by country
2. save model artifact by country to mongodb

## Prepare features
preparing aggregated features by country of users
```python
def prepare_features(client, 
                     analytics_db: str,
                     content_stats_collection: str,
                     creator_stats_collection: str):
    
        '''
        feature preparation using both "contentStats" & "creatorStats" then summary engagement behavior for each user
        '''

        # define cursor of content features
        contentFeaturesCursor = [
         {
            # join with creator stats
                '$lookup': {
                    'from': creator_stats_collection, # previous:'creatorStats',
                    'localField': 'authorId',
                    'foreignField': '_id',
                    'as': 'creatorStats'
                }
            }, {
            # deconstruct array
                '$unwind': {
                    'path': '$creatorStats',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
            # map output format
                '$project': {
                    '_id': 1,
                    'likeCount': 1,
                    'commentCount': 1,
                    'recastCount': 1,
                    'quoteCount': 1,
                    'photoCount': 1,
                    'characterLength': 1,
                    'creatorContentCount' :'$creatorStats.contentCount',
                    'creatorLikedCount': '$creatorStats.creatorLikedCount',
                    'creatorCommentedCount': '$creatorStats.creatorCommentedCount',
                    'creatorRecastedCount': '$creatorStats.creatorRecastedCount',
                    'creatorQuotedCount': '$creatorStats.creatorQuotedCount',
                    'ageScore': '$aggregator.ageScore'

                }
            }
        ]

        content_features = pd.DataFrame(list(client[analytics_db][content_stats_collection].aggregate(contentFeaturesCursor))).rename({'_id':'contentId'},axis = 1)
    
        return content_features

    contentFeatures = prepare_features(client = client, # default
                                        analytics_db = 'analytics-db',
                                        content_stats_collection = 'contentStats',
                                        creator_stats_collection = 'creatorStats')
    
    
    mlArtifacts_country = analyticsDb[saved_model]

    contentFeatures_1 = contentFeatures.fillna(0)
    trans_add = trans.merge(contentFeatures_1, on = 'contentId',how ='left')
    trans_add['label'] = trans_add['like']+trans_add['comment'] +trans_add['recast'] +trans_add['quote']  
    trans_add = trans_add
    
    select_user = trans_add.groupby('countryCode')['contentId'].agg('count').reset_index()
    select_user = select_user[select_user['contentId'] > 2]
```
## Save model artifact by users
saving model artifact by user into mongodb
```python
def save_model_to_mongodb(collection, model_name, account, model):

	'''
	upserts model artifact from model training into database
	'''

	pickled_model = pickle.dumps(model) # pickling the model

	document = collection.update_one(
		{
		'account': account,
		'model': str(model_name),
		}, {
		'$set': {
			'account': account,
			'model': str(model_name),
			'artifact': pickled_model,
			'trainedAt': datetime.utcnow(),
			'features' : list(Xlr.columns)
		}
		}, upsert= True)
```
