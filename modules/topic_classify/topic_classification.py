import os
import re
import itertools
import pandas as pd
# from mongo_client import mongo_client # uncomment this line when using 'lambda function'
#from lang_detector import lang_detect
from google.cloud import language_v1

def lang_detect(text: str):
    from langdetect import detect
    
    result_lang = detect(text)
    
    return result_lang

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./modules/topic_classify/gcp_data-science_service-account_key.json"

# define text cleaning using regex
def clean_text(data):
    
    # symbolic removing
    filter_cursor = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
                      "]+|http\S+|[\u0E00-\u0E7F']", re.UNICODE)
    
    pre_result = re.sub(filter_cursor, '', str(data))
    
    # whitespace removing
    symbol_filter_cursor = re.compile(r"[\n\!\@\#\$\%\^\&\*\-\+\:\;]")
    
    pre_result = symbol_filter_cursor.sub(" ", pre_result)

    # r/ removing
    rslash_filter_cursor = re.compile(r"r/")
    
    pre_result = rslash_filter_cursor.sub(" ", pre_result)
    
    # space removing
    space_filter_cursor = re.compile(r"\s+")
    
    result = space_filter_cursor.sub(" ", pre_result).strip()
    
    return result

# integrate data loading and query_to_df
def data_ingest(client, database_name='app-db', collection_name='contents'):
    
    # connect to database
    db = client[database_name]
    
    # loading data only for necessary fields i.e. '_id' & 'message'
    """
    in deploy environment, 'query statement' & 'projection statement' is required to 
    config as follow 'database trigger' parameter
    """
    
    # change parameter when using 'lambda function'
    df = pd.DataFrame.from_dict(list(db[collection_name].find({},{'_id':1 ,'text': '$payload.message'})))
    
    return df

def classify_text(text_content: str, _id: str) -> dict:
    
    """
    Classifying Content in a String
    Args:
      text_content The text content to analyze. Must include at least 20 words.
    """

    client = language_v1.LanguageServiceClient()

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
#    language = "en"
    language = lang_detect(text_content)
    document = {"content": text_content, "type_": type_, "language": language}

    response = client.classify_text(request = {'document': document})
    
    # Loop through classified categories returned from the API
    classify_result = {}
    
    #add more information
    classify_result['_id'] = _id
    classify_result['language'] = language
    
    for category in response.categories:
        
        # Get the name of the category representing the document.
        # See the predefined taxonomy of categories:
        
        # https://cloud.google.com/natural-language/docs/categories
#        print(u"Category name: {}".format(category.name))

        # Get the confidence. Number representing how certain the classifier
    
        # is that this category represents the provided text.
#        print(u"Confidence: {}".format(category.confidence))
        
        categories_name = category.name
        
        if categories_name:
            
            if categories_name.startswith('/'):
                # remove startswith /
                categories = categories_name[1:]
                categories = categories.split('/')
                
                categories_list = [] # empty list for collecting categories name
                
                # loop inside splitted categories
                for category_name in categories:
                    
                    # lower and replace '&' = 'and' & ' ' => '-'
                    categories_list.append(re.sub("\s+", "-", re.sub("&", "and", category_name)).lower())
                
        classify_result['categories'] = categories_list
        classify_result['confidence'] = category.confidence
        
    return classify_result

def implement_category_labeling(df):
    
    result = [] # empty list to collect results
    
    for index, rows in df.iterrows():
        
        # record data
        text = rows['text']
        _id = rows['_id']
        
        # tokenize text
        splitted = text.split(' ')
        
        # check threshold of word length
        # case of insufficient text to classify topic
        
        if len(splitted) < 21:
            
            try:
                # return only content id
                classify_result = {
                    '_id': _id
                    }
                
                result.append(classify_result) # store result
                
            except UnicodeEncodeError:
                
                continue

        # case of able to classify text
        else:
            try:
                # perform classify text
                classify_result = classify_text(text, _id)
                
                result.append(classify_result) # store result
                
            except UnicodeEncodeError: 
                
                continue
                
    return result

# define extract categories function
def extract_categories(labeled_df):
    
    # filter only categorizable documents
    categories_list = [document['categories'] for document in labeled_df if 'categories' in document]
    
    # drop duplicated
    categories_list = [categorized_df for categorized_df,_ in itertools.groupby(categories_list)]
    
    return categories_list

# define mapping function then upsert to collection 'topics'
# there are 2 minor consecutive functions i.e upsert raw slug & mapping object ids, respectively
def upsert_to_topic(client,
                    categories_list, 
                    database_name='analytics-db', 
                    collection_name='topics'): 
    
    # connect to database
    db = client[database_name]
    
    # minor function 1: upsert raw slug
    # looping trough categories_list
    for topics in categories_list:

        # looping trough sub categories
        for index, topic in enumerate(topics):

            # case: hiearachy
            if (len(topics) > 1):

                # sub-case: parent with children
                if index == 0:

                    temp = {'slug': topics[index], 'children': [topics[index + 1]]}

                    db[collection_name].update_one({'slug': temp['slug']}, [{
                        '$project': {
                            'childrenSlug': {'$setUnion': [{'$ifNull': ['$childrenSlug', []]}, temp['children']]},
                            'parents': 1,
                            'children': 1,
                            'slug': 1
                        }}], upsert=True)

                # sub-case: last children
                elif index == len(topics) - 1:

                    temp = {'slug': topics[index], 'parents': [topics[index - 1]]}

                    db[collection_name].update_one({'slug': temp['slug']}, [{
                        '$project': {
                            'parentsSlug': {'$setUnion': [{'$ifNull':['$parentsSlug', []]}, temp['parents']]},
                            'parents': 1,
                            'children': 1,
                            'slug': 1
                        }}], upsert=True)

                # sub-case: intermediate children
                else:

                    temp = {'slug': topics[index], 'parents': [topics[index - 1]], 'children': [topics[index + 1]]}

                    db[collection_name].update_one({'slug': temp['slug']}, [{
                        '$project': {
                            'childrenSlug': {'$setUnion': [{'$ifNull': ['$childrenSlug', []]}, temp['children']]},
                            'parentsSlug': {'$setUnion': [{'$ifNull':['$parentsSlug', []]}, temp['parents']]}, 
                            'parents': 1,
                            'children': 1,
                            'slug': 1
                        }}], upsert=True)

            # case: non-hiearachy    
            else:

                temp = {'slug': topics[index]}

                db[collection_name].update_one({'slug': temp['slug']}, [{'$project': {
                    'slug': 1, 
                    'parents': 1,
                    'children': 1,}}], upsert=True)
            
    # minor function 2: mapping object ids
    # define ObjectId addition cursor
    objectidMappingCursor = [
        {
            '$graphLookup': {
                'from': 'topics', 
                'startWith': '$parentsSlug', 
                'connectFromField': 'parentsSlug', 
                'connectToField': 'slug', 
                'as': 'parentsTemp', 
                'maxDepth': 0
            }
        }, {
            '$graphLookup': {
                'from': 'topics', 
                'startWith': '$childrenSlug', 
                'connectFromField': 'childrenSlug', 
                'connectToField': 'slug', 
                'as': 'childrenTemp', 
                'maxDepth': 0
            }
        }, {
            '$project': {
                '_id': 1, 
                'slug': 1,
                'children': 1, 
                'parents': 1, 
                'parentsTemp._id': 1, 
                'parentsTemp.slug': 1, 
                'childrenTemp._id': 1, 
                'childrenTemp.slug': 1
            }
        }, {
            '$project': {
                '_id': 1, 
                'slug': 1, 
                'children': {
                    '$setUnion': [
                        {
                            '$ifNull': [
                                '$children', []
                            ]
                        }, '$childrenTemp'
                    ]
                }, 
                'parents': {
                    '$setUnion': [
                        {
                            '$ifNull': [
                                '$parents', []
                            ]
                        }, '$parentsTemp'
                    ]
                }
            }
        }, {
            # upsert to 'topics' collection itself
            '$merge': {
                'into': {
                    'db': 'analytics-db', 
                    'coll': 'topics'
                }, 
                'on': '_id', 
                'whenMatched': 'replace', 
                'whenNotMatched': 'insert'
            }
        }
    ]
    
    # perform mapping object ids
    db[collection_name].aggregate(objectidMappingCursor)
    
    return None

# define mapping content id with topic id then upsert to 'contentTopics'
def upsert_to_content_topic(client,
                            labeled_df,
                            src_database_name='analytics-db', 
                            dst_database_name='analytics-db',
                            src_collection_name='topics',
                            dst_collection_name='contentTopics'):


    # connect to database
    src_db = client[src_database_name]
    dst_db = client[dst_database_name]

    for document in labeled_df:

        if 'categories' in document:

            topics = []

            for category in document['categories']:

                object_id = src_db[src_collection_name].find_one({'slug': category}, {'_id':1})

                topics.append(object_id['_id'])


            dst_db[dst_collection_name].update_one({'content': document['_id']}, [{
                                                '$project': {
                                                    'content': document['_id'],
                                                    'language': document['language'],
                                                    # maybe bug, its does not work
    #                                                 'confidence': document['confidence'],
                                                    'topics': topics
                                                }}], upsert=True)

        elif 'language' in document:

            dst_db[dst_collection_name].update_one({'content': document['_id']}, [{
                                                '$project': {
                                                    'content': document['_id'],
                                                    'language': document['language']
                                                }}], upsert=True)
    return None

# define main function
def topic_classify_main(mongo_client,
                        app_database_name='app-db', 
                        anlytc_database_name='analytics-db', 
                        src_ctnt_collection_name='contents',
                        src_tpc_collection_name='topics',
                        dst_tpc_collection_name='contentTopics'):
    import logging
    logging.info("Start topic classification")
    logging.debug('debug 1')
    # perform ingest data
    df = data_ingest(client=mongo_client, database_name=app_database_name, 
                     collection_name=src_ctnt_collection_name)
    logging.debug('debug 2')
    # perform clean text
    df['text'] = df['text'].map(clean_text)
    logging.debug('debug 3')
    # perform category labeling
    labeled_df = implement_category_labeling(df.head(10)) # remove .head() when using 'lambda function'
    logging.debug('debug 4')
    # extract categorise from labeled into list
    categories_list = extract_categories(labeled_df=labeled_df)
    logging.debug('debug 5')
    # perform upsert category to 'topics' master collection
    upsert_to_topic(client=mongo_client,
                    categories_list=categories_list, 
                    database_name=anlytc_database_name, 
                    collection_name=src_tpc_collection_name)
    logging.debug('debug 6')
    # perform upsert category to 'topics' master collection
    upsert_to_content_topic(client=mongo_client,
                            labeled_df=labeled_df,
                            src_database_name=anlytc_database_name, 
                            dst_database_name=anlytc_database_name,
                            src_collection_name=src_tpc_collection_name,
                            dst_collection_name=dst_tpc_collection_name)
    
    return None