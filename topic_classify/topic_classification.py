# -*- coding: utf-8 -*-
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# DO NOT EDIT! This is a generated sample ("Request",  "language_classify_text")

# To install the latest published package dependency, execute the following:
#   pip install google-cloud-language

# sample-metadata
#   title: Classify Content
#   description: Classifying Content in a String
#   usage: python3 samples/v1/language_classify_text.py [--text_content "That actor on TV makes movies in Hollywood and also stars in a variety of popular new TV shows."]

# [START language_classify_text]
from google.cloud import language_v1

def sample_classify_text(text_content: str, _id: str) -> dict:
    """
    Classifying Content in a String
    Args:
      text_content The text content to analyze. Must include at least 20 words.
    """
    from lang_detector import lang_detect

    client = language_v1.LanguageServiceClient()

    # text_content = 'That actor on TV makes movies in Hollywood and also stars in a variety of popular new TV shows.'

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
    classify_result['lang'] = language
    for category in response.categories:
        # Get the name of the category representing the document.
        # See the predefined taxonomy of categories:
        # https://cloud.google.com/natural-language/docs/categories
#        print(u"Category name: {}".format(category.name))
        # Get the confidence. Number representing how certain the classifier
        # is that this category represents the provided text.
#        print(u"Confidence: {}".format(category.confidence))
        # replace '/'
        category_name = category.name
        if category_name:
            if category_name.startswith('/'):
                # remove startswith /
                category_name = category_name[1:]
                category_name = category_name.split('/')
                
        classify_result['catogories'] = category_name
        classify_result['confidence'] = category.confidence
        
    return classify_result

def query_to_df(collection_name):
    from utils import MongoCTX
    from pprint import pprint
    import pandas as pd
    import bson.objectid
    
    def parse_to_df(cursor):
        
        df =  pd.DataFrame(list(cursor))
        
#        #import bson.object
#        df = df.astype(str)
        return df
    
    def dedup(df, col=None):
        
        df = df.drop_duplicates(subset=col)
        return df
    
    db = MongoCTX(db_name='analytics-db')
    cursor = db.qeury_find_all(collection_name=collection_name)
    
    df = parse_to_df(cursor=cursor)
    df = dedup(df, 'cleaned_message_with_hashtag')
    
    return df
    
def replace_whitespaces(text: str):
    import re
    
    _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
    my_str = _RE_COMBINE_WHITESPACE.sub(" ", text).strip()
    return my_str

def classify(df):
    import pandas as pd
    
    result = []
#    for index, rows in df.iterrows():
    for index, rows in df.iterrows():
        # record data
        text = rows['cleaned_message_with_hashtag']
        _id = rows['_id']
        
        # clean text before predict
        text = replace_whitespaces(text)
        splitted = text.split(' ')
        if len(splitted) < 21:
            continue
        else:
            try:
                classify_result = sample_classify_text(text, _id)
                # store result
                result.append(classify_result)
            except UnicodeEncodeError:
                continue
    return result


def main():
    df = query_to_df(collection_name='contentCleaned')
    result = classify(df)
    print(result)


if __name__ == "__main__":
    main()