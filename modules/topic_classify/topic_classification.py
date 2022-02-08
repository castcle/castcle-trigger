'''
version: 2.0
modules topic classification
function
    1. ingest data
    2. detect topics & language
    3. upsert to databases
'''
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
from langdetect.lang_detect_exception import LangDetectException

# assign credential for google cloud platform
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './gcp_data_science_service_account_key.json'

# obtain desirable data format from event
def data_ingest(event):
    
    '''
    reformat event data then convert into dataframe
    '''

    # reformat by deconstruct nest json
    temp = {
        '_id': ObjectId(event['detail']['fullDocument']['_id']),
        'message': event['detail']['fullDocument']['payload']['message'],
        'updatedAt': parser.parse(event['detail']['fullDocument']['updatedAt'])
    }
    
    # convert event document to dataframe
    reformatted_dataframe = pd.DataFrame.from_dict([temp])
    
    return reformatted_dataframe

# define text cleaning using regex
def clean_text(message: str):

    '''
    clean text by removing special characters, emojis
    '''
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
    
    # whitespace & bullets removing
    symbol_filter_pattern = re.compile(r"[\n\!\@\#\$\%\^\&\*\-\+\:\;\.\u2022,\u2023,\u25E6,\u2043,\u2219]")
    
    pre_result = symbol_filter_pattern.sub(" ", pre_result)

    # r/ removing
    rslash_filter_pattern = re.compile(r"r/")

    pre_result = rslash_filter_pattern.sub(" ", pre_result)
    
    # space removing
    space_filter_pattern = re.compile(r"\s+")
    
    cleaned_text = space_filter_pattern.sub(" ", pre_result).strip()
    
    return cleaned_text

# detect language
def lang_detect(text: str):
    from langdetect import detect
    
    result_lang = detect(text)
    
    return result_lang

# TH detector
def gcld(text: str):
    import gcld3
    
    detector = gcld3.NNetLanguageIdentifier(min_num_bytes=10, max_num_bytes=1000)
    results = detector.FindLanguage(text=text)
    
    lang = results.language
    reliable = results.is_reliable
    proportion = results.proportion
    probability = results.probability
    
    return lang, reliable

# topic classify from text
def classify_text(message: str,
                  _id, 
                  language: str, 
                  updatedAt) -> dict:
    
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
    document = {
        "content": message, 
        "type_": type_, 
        "language": language
        }

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

def call_translation_api(message) -> str:
    """
    Call Google NLP translator https://cloud.google.com/translate

    Translate every languages to english

    input: 
        message to be translated
    output:
        translated message, ENGLISH
    """

    def translate_text_with_model(target, text, model="nmt") -> str:
        """Translates text into the target language.

        Make sure your project is allowlisted.

        Target must be an ISO 639-1 language code.
        See https://g.co/cloud/translate/v2/translate-reference#supported_languages
        """
        from google.cloud import translate_v2 as translate

        translate_client = translate.Client()

        if isinstance(text, bytes):
            text = text.decode("utf-8")

        # Text can also be a sequence of strings, in which case this method
        # will return a sequence of results for each text.
        result = translate_client.translate(text, target_language=target, model=model)

        print(u"Text: {}".format(result["input"]))
        print(u"Translation: {}".format(result["translatedText"]))
        print(u"Detected source language: {}".format(result["detectedSourceLanguage"]))
        return result["translatedText"]

    # if message is not english, we will translate it to English
    translatedText = translate_text_with_model(target="en", text=message)

    return translatedText

# implement both languge & topic labeling
def message_classify(reformatted_dataframe):
    
    '''
    calls clean text function together with topic classify function as condition as follow, 
    1. message contains Thai character => language = "th" and no topic
    2. message is not English language => language = <detected language> and no topic
    3. message is English language => language = "en" and,
        3.1 contains more than "message_length_threshold" => classify topics
        3.2 contains more than "message_length_threshold" => no topic
    4. message is unknown langage => language = "n/a" and no topic
    '''

    def ggl_api_chk_rdy(message) -> bool:
        """
        Check message whether it is ready for calling google classify API

        Input:
            message (content)
        Output:
            ready or not (True or False)
        """

        # define threshold
        message_length_threshold = 21 # changed from 20
        # tokenize text by slice for 1st row (input has only single row)

        splitted = message.split(' ')

        cannot_use_google_classify = True \
                        if len(splitted) < message_length_threshold else False

        return cannot_use_google_classify

    
    
    # perform clean text
    _id = reformatted_dataframe['_id'][0]
    updatedAt = reformatted_dataframe['updatedAt'][0]
    message = clean_text(reformatted_dataframe['message'][0])

    # extract language
    print('message is:')
    print(reformatted_dataframe['message'][0])
    print(repr(reformatted_dataframe['message'][0]))

    # regex thai letters
    pattern = re.compile(u"[\u0E00-\u0E7F]")

    # Thai language case
    if len(re.findall(pattern, reformatted_dataframe['message'][0])) > 0:

        print('Thai letter(s) found')

        lang, reliable = gcld(message)
        
        if lang == 'th' and reliable == True:
            # case TH reliable
            language = lang
        else:
            print('not reliable')
            language = 'th'

    # unknown language
    else:

        print('Thai letter(s) not found')

        # case non-Thai but detectable language
        try:
            # change to gcld3
            lang, reliable = gcld(message)
            language = lang
    
        # case non-Thai and undetectable language
        except Exception as e:
            print("[Exception] Error:", e)
            print("[Exception] message", message)
            language = "n/a"

    print('[INFO] language:', language) #! just for mornitoring

    cannot_use_google_classify = ggl_api_chk_rdy(message)
    
    # use google language API only if language = English
    if language == 'en':
        
        # check threshold of word length
        # case of insufficient text to classify topic
        if cannot_use_google_classify:

            # return only content id
            topics_list = {'_id': _id,
                           'language': language,
                           'updatedAt': updatedAt}

        # case of able to classify text
        else:
            try:
                #! log
                print('classifying message:', message)
                # perform classify text
                topics_list = classify_text(message, _id, language, updatedAt)

                print('topics:', topics_list) #! just for mornitoring

            except UnicodeEncodeError as error: 
                print(f"[Exception] {error}")
                pass
    # non english
    else:
        # if translatedText is True do ... stuff
        translatedText = call_translation_api(message)

        cannot_use_google_classify = ggl_api_chk_rdy(translatedText)

        if cannot_use_google_classify:

            # return only content id
            topics_list = {'_id': _id,
                            'language': language,
                            'updatedAt': updatedAt}
        # If the translated message can call Google Classify API
        elif not cannot_use_google_classify:

            try:
                #! log
                print('classifying message:', message)
                # perform classify text
                topics_list = classify_text(message, _id, language, updatedAt)

                print('topics:', topics_list) #! just for mornitoring

            except UnicodeEncodeError as error: 
                print(f"[Exception] {error}")
                pass
    
    return topics_list 

# define mapping function then upsert to collection 'topics'
# there are 2 minor consecutive functions i.e upsert raw slug & mapping object ids, respectively
def upsert_to_topics(topics_list, 
                    topic_database_name: str, 
                    topic_collection_name: str): 

    '''
    if message has topics/categories is detected, reformat to json for each topic and assigns children/parents relationship then upsert to database twice then upserts topics into database hierachically to stamp topic slugs
    '''
    
    # case 'categories' present in 'topic_list'
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
                # filter of recent update contents
                '$match': {
                    'updatedAt': updatedAt
                }
            }, {
                # find related parent topics
                '$graphLookup': {
                    'from': 'topics', 
                    'startWith': '$parentsSlug', 
                    'connectFromField': 'parentsSlug', 
                    'connectToField': 'slug', 
                    'as': 'parentsTemp', 
                    'maxDepth': 0 # ~ degree of separation
                }
            }, {
                # find related children topics
                '$graphLookup': {
                    'from': 'topics', 
                    'startWith': '$childrenSlug', 
                    'connectFromField': 'childrenSlug', 
                    'connectToField': 'slug', 
                    'as': 'childrenTemp', 
                    'maxDepth': 0 # ~ degree of separation
                }
            }, {
                # map output format
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
                # map output format again, due to mongodb limitation
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

    '''
    upsert topic object IDs and language to database,
        - if topic is detected, finds correspond object ID then insert into database
        - insert language to the same database
    '''

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
        
    '''
    main function of topic classification
    1. ingest data
    2. detect topics & language
    3. upsert to databases
    ''' 

    logging.info("Start topic classification")
    
    # 1. ingest data
    logging.debug('debug 1')
    
    ## perform ingest data
    reformatted_dataframe = data_ingest(event)

    print('input data:', reformatted_dataframe) #!! checkpoint
    
    # 2. detect topics & language
    logging.debug('debug 2')
    
    ## perform category labeling
    topics_list = message_classify(reformatted_dataframe)

    print('topics is:', topics_list) #!! checkpoint
    
    # 3. upsert to databases
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

    # end of implementation, below paragraphs are logging
    print('topic classification of content id:', reformatted_dataframe['_id'][0], 'done') #!! checkpoint

    # observe output
    print('content id & message:', topics_list['_id'])
    print(list(mongo_client['app-db']['contents'].find({'_id': topics_list['_id']}, {'payload.message': 1})))

    print('content info:', topics_list['_id'])
    print(list(mongo_client['app-db']['contentinfo'].find({'contentId': topics_list['_id']})))

    if 'categories' in topics_list:
        print('topics (filtered by updatedAt):', topics_list['updatedAt'])
        print(list(mongo_client['analytics-db']['topics'].find({'slug': {'$in': topics_list['categories']}})))
    
    return None