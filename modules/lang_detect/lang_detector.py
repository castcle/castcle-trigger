def spacy_detector():
    import spacy_fastlang

    nlp = spacy.load("en_core_web_sm")
    nlp.add_pipe("language_detector")
    doc = nlp('Life is like a box of chocolates. You never know what you are gonna get.')

    assert doc._.language == 'en'
    assert doc._.language_score >= 0.8
    
def textblob_detector():
    from textblob import TextBlob
    
    text = "это компьютерный портал для гиков. It was a beautiful day ."
    lang = TextBlob(text)
    print(lang.detect_language())
    
def lang_detect(text: str):
    from langdetect import detect
    
    result_lang = detect(text)
    
    return result_lang

if __name__ == '__main__':
    import sys
    
    text = sys.argv[1]
    lang = lang_detect(text)
    print(lang)