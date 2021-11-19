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


# assign credential for google cloud platform
gcp_key_64 = os.environ["GCP_KEY"]
_GOOGLE_APPLICATION_CREDENTIALS = base64.b64decode(gcp_key_64).decode("utf-8") 
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./modules/topic_classify/gcp_data-science_service-account_key.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _GOOGLE_APPLICATION_CREDENTIALS


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
    message_length_threshold = 20
    
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
        mongo_client[contents_database_name][contents_collection_name].update_one({'_id': _id}, [{
                                    '$set': {
                                        'language': language,
                                        'topics': topic_ids
                                    }}], upsert=True) # change to True when using contents
        
    # case get language but not categories
    elif 'language' in topics_list:
        
        language = topics_list['language']

        # update to original content
        mongo_client[contents_database_name][contents_collection_name].update_one({'_id': _id}, [{
                                            '$set': {
                                                'language': language,
                                            }}], upsert=True) # change to True when using contents
    
    return None

# comment this due to 'dev' will handle hashtags
# # define hashtag extract from 'contents' => 'hashtags' & 'content.hastags' function
# def hashtag_extract(df):
    
#     _id = df['_id'][0]
#     message = df['message'][0]
#     updatedAt = df['updatedAt'][0]
    
#     hashtags_list = {} # assign empty variable
    
#     # define regex pattern to extract hashtag(s) from event document 
#     hastag_pattern = re.compile(r"(?<=#)[a-zA-Z0-9]+")
    
#     hashtags = re.findall(hastag_pattern, message)
    
#     if len(hashtags) != 0:
    
#         hashtags_list['_id'] = _id
#         hashtags_list['hashtags'] = hashtags
#         hashtags_list['updatedAt'] = updatedAt
        
#     else:
        
#         hashtags_list['_id'] = _id
    
#     return hashtags_list

# def upsert_to_hashtags_and_update_contents(hashtags_list,
#                       contents_database_name: str, # original database which will be add hashtags field to content
#                       contents_collection_name: str, # original collection which will be add hashtags field to content 
#                       hashtags_database_name: str, # destination database which is consider as master collection
#                       hashtags_collection_name: str): # destination collection which is consider as master collection
    
#     if 'hashtags' in hashtags_list:
    
#         _id = hashtags_list['_id']
#         hashtags = hashtags_list['hashtags']
#         updatedAt = hashtags_list['updatedAt']

#         # condition for accept only event document that is able to extract hashtag(s)
#         if len(hashtags) != 0:

#             hashtag_ids = [] # assign empty list to collect object id(s) of hashtag(s) through for loop

#             # for loop through each element of hashtags
#             for hashtag_capital in hashtags:

#                 hashtag = hashtag_capital.lower() # decapitalize to slug form

#                 # update | insert to 'hashtags' master collection
#                 mongo_client[hashtags_database_name][hashtags_collection_name].update_one({'slug': hashtag}, [{
#                     '$project': {
#                         'slug': hashtag,
#                         'createdAt': {'$ifNull': ['$createdAt', updatedAt]},
#                         'updatedAt': updatedAt
#                     }}], upsert=True)

#                 # append the assigned list
#                 hashtag_ids.append(mongo_client[hashtags_database_name][hashtags_collection_name].find_one({'slug': hashtag}, {'_id': 1})['_id'])

#             # update by adding 'hashtags' field to original collection of event document with the appended list
#             mongo_client[contents_database_name][contents_collection_name].update_one({'_id': _id}, [{
#                 '$set': {
#                     'hastags': hashtag_ids
#                 }}], upsert=False)
        
#     return None

#######################################################################
# #! just in testing stage -> create parallele document in 'contents_test'
# def parallele_insert(event,
#                      dst_database_name='analytics-db',
#                      dst_collection_name='contents_test'):
    
#     # reformat from event => document
#     parallele_document = {
    
#     '_id': event['documentKey']['_id'],
#     'payload': {
#         'message': event['fullDocument']['payload']['message']
#     },
#     'updatedAt': event['fullDocument']['updatedAt']
#                      }
    
#     mongo_client[dst_database_name][dst_collection_name].insert_one(parallele_document)

#     return None

# define main function
def topic_classify_main(event,   
                        topic_database_name='analytics-db', 
                        topic_collection_name='topics',
                        # hashtags_database_name = 'analytics-db', # comment this due to 'dev' will handle hashtags
                        # hashtags_collection_name = 'hashtags', # comment this due to 'dev' will handle hashtags
                        # contents_database_name = 'analytics-db', #! test, remove this then uncomment below
                        # contents_collection_name = 'contents_test'): #! test, remove this then uncomment below
                        contents_database_name = 'app-db',
                        contents_collection_name = 'contents'):
        
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

    # print('topics is:', topics_list) #!! checkpoint
    
    # comment this due to 'dev' will handle hashtags
    # ## perform hashtag extraction
    # hashtags_list = hashtag_extract(df)
    
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

    # print('upsert to content_test done') #!! checkpoint

    # comment this due to 'dev' will handle hashtags
    # logging.debug('debug 5')
    
    # ## perform upsert hashtags to 'hashtags' master collection then update original content by adding 'hashtags' field
    # upsert_to_hashtags_and_update_contents(hashtags_list,
    #                   contents_database_name, # original database which will be add hashtags field to content
    #                   contents_collection_name, # original collection which will be add hashtags field to content 
    #                   hashtags_database_name, # destination database which is consider as master collection
    #                   hashtags_collection_name)


    print('topic classification of content id:', df['_id'][0], 'done') #!! checkpoint
    
    return None