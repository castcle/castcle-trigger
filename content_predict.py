import json
from mongo_client import mongo_client

db = mongo_client['analytics-db']

def handle(event, context):
    print(json.dumps(event, indent=4))
    from lang_detect.lang_detector import lang_detect
    from topic_classify.topic_classification import classify_text
    
    _id = event['detail']['fullDocument']['_id']
    text = event['detail']['fullDocument']['payload']['message']
    
    # lang detect
    lang = lang_detect(text_content=text)
    
    # topic predict
    classify_result =  classify_text(text_content=text, _id=_id)

    return {
        '_id': _id,
        'text': text,
        'lang': lang,
        'category': classify_result['catogories'],
        'confidence': classify_result['confidence']
    }