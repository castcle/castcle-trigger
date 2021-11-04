import json
from mongo_client import mongo_client

db = mongo_client['analytics-db']
def detect(text_content):
    from lang_detector import lang_detect
    
    language = lang_detect(text_content)
    
    return language

def handle(event, context):
    print(json.dumps(event, indent=4))
    
    text = event['content']
    
    # lang detect
    lang = detect(text_content=text)
    
    # topic predict
    

    return {
        'text': text,
        'lang': lang
    }