# Topic Classfier module
Classify content when user create content on their account
## scenario
### image

1. user create content in castcle app
2. content created in mongodb
3. mongodb trigger to aws event bridge
4. aws event bridge invoke lambda with message contains content
5. content language detect then save the result to ...
6. content topic classify then save the result to ...

## Parse event message
the code below parse message from mongodb trigger to pandas dataframe
```python
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
```

## Language Detect and Classify
To use language detector and topic classifier, you need to clean the unwanted 
character in content like special character
```python
# implement both languge & topic labeling
def get_topic_document(df):
    
    # define threshold
    message_length_threshold = 21 # changed from 20
    
    # perform clean text
    _id = df['_id'][0]
    updatedAt = df['updatedAt'][0]
    message = clean_text(df['message'][0])

    # extract language
  
    print('message is:')
    print(df['message'][0])
    print(repr(df['message'][0]))

    # regex thai letters
    pattern = re.compile(u"[\u0E00-\u0E7F]")

    # Thai language case
    if len(re.findall(pattern, df['message'][0])) > 0:

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


            language = lang_detect(message)
    
        # case non-Thai and undetectable language
        except LangDetectException:
    
            language = "n/a"

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
```

## Helper functions
```python
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
    # bullets removing
    symbol_filter_pattern = re.compile(r"[\n\!\@\#\$\%\^\&\*\-\+\:\;\u2022,\u2023,\u25E6,\u2043,\u2219]")
    

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
```