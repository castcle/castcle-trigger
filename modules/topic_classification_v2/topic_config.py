# # create USE embedding by using tensorflow hub
import tensorflow_text 
import tensorflow_hub as hub 
embed = hub.load("https://tfhub.dev/google/universal-sentence-encoder-multilingual/3")

topic_list = [
    # ordinary topic
    'Smartphones',
    'Business',
    'Entertainment',
    'Politics',
    'Science',
    'Colleges',
    'Country',
    'Technology',
    'Weather',
    'Food and health',
    'Financial',
    'Exercise',
    'Photography',
    'Horoscope',
    'Sports',
    'Travel',
    'Celebrity',
    'Shopping',
    'Cryptocurrency',
    'Religion',
    'Television programmes',    
    # bad topic
    'Drug',
    'Weapon',
    'Pornographic',
    'Violence',
    'Hate speech'    
    ]
