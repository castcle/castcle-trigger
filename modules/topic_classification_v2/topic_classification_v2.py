import pandas as pd
import tensorflow as tf 
import tensorflow_text 

def clean_text(txt_):
    """
    remove url, hashtag, newline. emoji, doublespace
    """
    import re
    detect_url = '((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*'
    txt_ = re.sub(detect_url, '', txt_)
    detect_hashtag = r"#(\w+)"
    txt_ = re.sub(detect_hashtag, '', txt_)
    detect_newline = r'(\n+)'
    txt_ = re.sub(detect_newline, '', txt_)
    detect_emoji = r'\d+(.*?)(?:\u263a|\U0001f645)'
    txt_ = re.sub(detect_emoji, '', txt_)
    detect_emoji2 = r"(u00a9u00ae[u2000-u3300]ud83c[ud000-udfff]ud83d[ud000-udfff]ud83e[ud000-udfff])"
    txt_ = re.sub(detect_emoji2, "", txt_)
    detect_doublespace = r' +'
    txt_ = re.sub(detect_doublespace, ' ', txt_).strip()
    return txt_

def update_topic(
                contentId, 
                topic, 
                score, 
                client,
                target_db,
                target_collection
                ):
    """
    Update columns instend insert
    """
    filter = { 'contentId': contentId}
    print(filter)
    # Values to be updated.
    newvalues = { "$set": {
                'topic_classify': {'class':topic,'score':score}
                }}

    # Using update_one() method for single
    # updation.
    mycol_contentfiltering = client[target_db][target_collection]
    mycol_contentfiltering.update_one(filter, newvalues)

def embedding_interest(embed, word_list):
    # convert word to dataframe
    other_interest = pd.DataFrame({'other_interest': word_list})
    emb_other_interest = embed(other_interest['other_interest'].values).numpy()
    # embedding
    df_emb_oth_interest = pd.DataFrame(emb_other_interest, index = other_interest['other_interest'].values)
    return df_emb_oth_interest


def embedding_topic(embed, df_emb_oth_interest, topic_list):
    # embedding
    df_emb_category = pd.DataFrame(embed(topic_list).numpy(), index = topic_list)
    # get topicclass and score
    argmax_category = df_emb_oth_interest.dot(df_emb_category.T).idxmax('columns').values
    argmax_score = df_emb_oth_interest.dot(df_emb_category.T).max(axis=1).values
    # create df
    df_result = pd.DataFrame({'original_text': df_emb_oth_interest.index, 'category' : argmax_category, 'score' : argmax_score})
    return df_result


def universal_sentence_encoder(embed, word_list, topic_list):
    """
    input = list -> convert to pandas
    output = return topic in string
    """
    # embedding interest
    df_emb_oth_interest = embedding_interest(embed, word_list)
    # embedding topic
    df_result = embedding_topic(embed, df_emb_oth_interest, topic_list)
    # thredshold score
    mask = df_result.score < 0.15
    df_result.loc[mask,  'category'] = 'etc'
    df_result.loc[mask,  'score'] = 0
    return df_result

def topic_classify_main(
                        client,
                        target_db,
                        target_collection
                        ):
    """
    This is topic classify main function
    Query content that not have topic, Predict topic and update each content.
    """
    from modules.topic_classification_v2.topic_config import embed
    from modules.topic_classification_v2.topic_config import topic_list
    # retrive content 
    mycol_contentfiltering = client[target_db][target_collection]
    mycol_contentfiltering = list(mycol_contentfiltering.aggregate([
                        {'$match': {'topic': None}}, 
                            {'$project': {'_id':0, 'contentId':1, 'massageInEN':1, 'topic':1}}
                            ,{'$limit':100000}
                                                            ]))
    mycol_contentfiltering_df = pd.DataFrame(mycol_contentfiltering)
    mycol_contentfiltering_df['topic'] = None
    mycol_contentfiltering_df['massageInEN'] = mycol_contentfiltering_df['massageInEN'].apply(clean_text)
    mycol_contentfiltering_df = mycol_contentfiltering_df.rename(columns={'contentID': 'contentId'})
    print("Query content filtering that not have topic.")
    print(len(mycol_contentfiltering_df))

    # for loop predict topic, then updated.
    print("Loop predict topic, then updated.")
    for i in range(len(mycol_contentfiltering_df)):
        contentId = mycol_contentfiltering_df['contentId'][i]
        try:
            # run topic classify if word.split >1
            if len(mycol_contentfiltering_df['massageInEN'][i].split(' ')) > 1:
                massageInEN = [mycol_contentfiltering_df['massageInEN'][i]]
                result = universal_sentence_encoder(embed, massageInEN, topic_list)
                topic = result['category'][0]
                score = round(float(result['score'][0]),2)
            else:
                topic = 'etc'
                score = 0.0
            #print(contentId, topic, score)
            update_topic(contentId, topic, score, client, target_db, target_collection)
        except Exception as e: 
            print('error content:', contentId)
            print(e)
            pass
    print("Done update topic classify.")

