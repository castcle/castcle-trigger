import os
import re
import itertools
import logging
import json
import bson.objectid
from bson import ObjectId
import pandas as pd
from dateutil import parser
from google.cloud import language_v1
from mongo_client import mongo_client
import base64
import boto3

'''
# try 1
# assign credential for google cloud platform
gcp_key_64 = os.environ["GCP_KEY"]
_GOOGLE_APPLICATION_CREDENTIALS = base64.b64decode(gcp_key_64).decode("utf-8") 
GCP_obj = json.dumps(_GOOGLE_APPLICATION_CREDENTIALS)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_obj
'''

# assign credential for google cloud platform
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './gcp_data_science_service_account_key.json'

# # try 2
# client = boto3.client('s3')
# response = client.get_object( Bucket='ml-dev.castcle.com', Key='gcp_data-science_service-account_key.json')
# body = response['Body'].read().decode('utf-8')
# json_content = json.loads(body)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_content

# integrate data loading and query_to_df
def data_ingest(event):
    
    # reformat by deconstruct nest json
    temp = {
        '_id': ObjectId(event['detail']['fullDocument']['_id']),
        'message': event['detail']['fullDocument']['payload']['message'],
        'updatedAt': parser.parse(event['detail']['fullDocument']['updatedAt'])
    }
    
    # convert event document to dataframe
    df = pd.DataFrame.from_dict([temp])
    
    return df

# define text cleaning using regex
def clean_text(message: str):
    
    # symbolic removing
    filter_pattern = re.compile("["
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
    
    pre_result = re.sub(filter_pattern, '', message)
    
    # whitespace removing
    symbol_filter_pattern = re.compile(r"[\n\!\@\#\$\%\^\&\*\-\+\:\;]")
    
    pre_result = symbol_filter_pattern.sub(" ", pre_result)

    # r/ removing
    rslash_filter_pattern = re.compile(r"r/")
    pre_result = rslash_filter_pattern.sub(" ", pre_result)
    
    # space removing
    space_filter_pattern = re.compile(r"\s+")
    
    clean_result = space_filter_pattern.sub(" ", pre_result).strip()
    
    return clean_result

# detect language
def lang_detect(text: str):
    from langdetect import detect
    
    result_lang = detect(text)
    
    return result_lang

# topic classify from text
def classify_text(message: str, _id, language: str, updatedAt) -> dict:
    
    """
    Classifying Content in a String
    Args:
      message/text_content The text content to analyze. Must include at least 20 words.
    """
    client = language_v1.LanguageServiceClient()

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    document = {"content": message, "type_": type_, "language": language}

    response = client.classify_text(request = {'document': document})
    
    # Loop through classified categories returned from the API
    classify_result = {}
    
    #add more information
    classify_result['_id'] = _id
    classify_result['language'] = language  
    
    # use google language API only if language = English
    if language == 'en':
    
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

                        # slug construction; lower and replace '&' = 'and' & ' ' => '-'
                        categories_list.append(re.sub("\s+", "-", re.sub("&", "and", category_name)).lower())

                classify_result['categories'] = categories_list
                classify_result['confidence'] = category.confidence
                classify_result['updatedAt'] = updatedAt
        
    return classify_result

# implement both languge & topic labeling
def get_topic_document(df):
    
    # define threshold
    message_length_threshold = 21 # changed from 20
    
    # perform clean text
    _id = df['_id'][0]
    updatedAt = df['updatedAt'][0]
    message = clean_text(df['message'][0])
    
    language = lang_detect(message)

    print('language:', language) #! just for mornitoring
    
    # tokenize text by slice for 1st row (input has only single row)
    splitted = message.split(' ')
    
    # use google language API only if language = English
    if language == 'en':
        
        # check threshold of word length
        # case of insufficient text to classify topic
        if len(splitted) < message_length_threshold:

            # return only content id
            topics_list = {'_id': _id,
                           'language': language,
                           'updatedAt': updatedAt}

        # case of able to classify text
        else:

            try:
                # perform classify text
                topics_list = classify_text(message, _id, language, updatedAt)

                print('topics:', topics_list) #! just for mornitoring

            except UnicodeEncodeError: 

                pass
    else:
        
            # return only content id
            topics_list = {'_id': _id,
                           'language': language,
                           'updatedAt': updatedAt}
    
    return topics_list 

# define mapping function then upsert to collection 'topics'
# there are 2 minor consecutive functions i.e upsert raw slug & mapping object ids, respectively
def upsert_to_topics(topics_list, 
                    topic_database_name: str, 
                    topic_collection_name: str): 
    
    if 'categories' in topics_list:
    
        # minor function 1: upsert raw slug
        # assign fields to variables
        topics = topics_list['categories']
        updatedAt = topics_list['updatedAt']

        # looping trough sub categories
        for index, topic in enumerate(topics):

            # case: hiearachy
            if (len(topics) > 1):

                # sub-case: parent with children
                if index == 0:

                    temp = {'slug': topics[index], 'children': [topics[index + 1]]}

                    mongo_client[topic_database_name][topic_collection_name].update_one({'slug': temp['slug']}, [{
                        '$project': {
                            'childrenSlug': {'$setUnion': [{'$ifNull': ['$childrenSlug', []]}, temp['children']]},
                            'parents': 1,
                            'children': 1,
                            'slug': 1,
                            'createdAt': {'$ifNull': ['$createdAt', updatedAt]},
                            'updatedAt': updatedAt 
                        }}], upsert=True)




                # sub-case: last children
                elif index == len(topics) - 1:

                    temp = {'slug': topics[index], 'parents': [topics[index - 1]]}

                    mongo_client[topic_database_name][topic_collection_name].update_one({'slug': temp['slug']}, [{
                        '$project': {
                            'parentsSlug': {'$setUnion': [{'$ifNull':['$parentsSlug', []]}, temp['parents']]},
                            'parents': 1,
                            'children': 1,
                            'slug': 1,
                            'createdAt': {'$ifNull': ['$createdAt', updatedAt]},
                            'updatedAt': updatedAt
                        }}], upsert=True)

                # sub-case: intermediate children
                else:

                    temp = {'slug': topics[index], 'parents': [topics[index - 1]], 'children': [topics[index + 1]]}

                    mongo_client[topic_database_name][topic_collection_name].update_one({'slug': temp['slug']}, [{
                        '$project': {
                            'childrenSlug': {'$setUnion': [{'$ifNull': ['$childrenSlug', []]}, temp['children']]},
                            'parentsSlug': {'$setUnion': [{'$ifNull':['$parentsSlug', []]}, temp['parents']]}, 
                            'parents': 1,
                            'children': 1,
                            'slug': 1,
                            'createdAt': {'$ifNull': ['$createdAt', updatedAt]},
                            'updatedAt': updatedAt
                        }}], upsert=True)

            # case: non-hiearachy    
            else:

                temp = {'slug': topics[index]}

                mongo_client[topic_database_name][topic_collection_name].update_one({'slug': temp['slug']}, [{'$project': {
                    'slug': 1, 
                    'parents': 1,
                    'children': 1,
                    'createdAt': {'$ifNull': ['$createdAt', updatedAt]},
                    'updatedAt': updatedAt
                }}], upsert=True)

        # minor function 2: mapping object ids
        # define ObjectId addition cursor
        objectidMappingCursor = [
            {
                '$match': {
                    'updatedAt': updatedAt
                }
            }, {
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
                    'childrenTemp.slug': 1,
                    'createdAt': 1,
                    'updatedAt': 1
                }
            }, {
                '$project': {
                    '_id': 1, 
                    'slug': 1, 
                    'createdAt': 1,
                    'updatedAt': 1,
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
                        'db': topic_database_name, 
                        'coll': topic_collection_name
                    }, 
                    'on': '_id', 
                    'whenMatched': 'replace', 
                    'whenNotMatched': 'insert'
                }
            }
        ]

        # perform mapping object ids
        mongo_client[topic_database_name][topic_collection_name].aggregate(objectidMappingCursor)

    return None

# define mapping content id with topic id then upsert to 'contentTopics'
def upsert_topics_to_contents(topics_list,
                              topic_database_name: str, 
                              topic_collection_name: str,
                              contents_database_name: str,
                              contents_collection_name: str):
    
    # assign fields to variables
    _id = topics_list['_id']
    
    # check topic existence
    if 'categories' in topics_list:
        
        language = topics_list['language']
        categories = topics_list['categories']

        # assign empty list to collect object ids
        topic_ids = []

        # case get categories
        for category in categories:
    
            # collect object ids throgh the loop
            topic_ids.append(mongo_client[topic_database_name][topic_collection_name].find_one({'slug': category}, {'_id':1})['_id'])

        # update to original content
        mongo_client[contents_database_name][contents_collection_name].update_one({'contentId': _id}, [{
                                    '$set': {
                                        'contentId': _id,
                                        'language': language,
                                        'topics': topic_ids
                                    }}], upsert=True) # change to True when using contents
        
    # case get language but not categories
    elif 'language' in topics_list:
        
        language = topics_list['language']

        # update to original content
        mongo_client[contents_database_name][contents_collection_name].update_one({'contentId': _id}, [{
                                            '$set': {
                                                'contentId': _id,
                                                'language': language,
                                            }}], upsert=True) # change to True when using contents
    
    return None



# define main function
def topic_classify_main(event,   
                        topic_database_name:str, 
                        topic_collection_name:str,
                        contents_database_name:str,
                        contents_collection_name:str):
        
    logging.info("Start topic classification")

    # print('Start topic classification') #!! checkpoint

    # #! 0. just for testing stage -> remove this when stable
    # parallele_insert(event)
    
    # 1. loading data
    logging.debug('debug 1')
    

    ## perform ingest data
    df = data_ingest(event)

    # print('df:', df) #!! checkpoint
    
    # 2. data processing
    logging.debug('debug 2')
    
    ## perform category labeling
    topics_list = get_topic_document(df)

    print('topics is:', topics_list) #!! checkpoint
    
    # 3. upload to databases
    
    logging.debug('debug 3')
    
    ## perform upsert category to 'topics' master collection
    upsert_to_topics(topics_list, 
                    topic_database_name=topic_database_name, 
                    topic_collection_name=topic_collection_name)

    # print('upsert to topics done') #!! checkpoint                
    
    logging.debug('debug 4')

    ## update original content by adding 'topics' field 
    upsert_topics_to_contents(topics_list,
                              topic_database_name=topic_database_name,
                              topic_collection_name=topic_collection_name,
                              contents_database_name=contents_database_name,
                              contents_collection_name=contents_collection_name)

    print('topic classification of content id:', df['_id'][0], 'done') #!! checkpoint
    
    return None