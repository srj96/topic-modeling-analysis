import numpy as np
import pandas as pd 
import requests
import json 
import argparse
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from data_preprocessing import DataPreproc
from topic_model_algo import TopicAlgo
# from concat_csv_data import df_all

class BaseClass:
    def __init__(self,**kwargs):
        if 'text_body' in kwargs:
            self.body = kwargs['text_body']
        # if 'keyword' in kwargs:
        #     self.key = kwargs['keyword']
        
        news_body_df = self.body
        news_body_df.rename(columns={'Body' : '_source.body'}, inplace = True)

        news_body_df = news_body_df.dropna(subset = ['_source.body'])
        
        # news_body_df = news_body_df.fillna({'_source.body':''})
        # news_body_df = news_body_df[news_body_df['body'].str.contains(r"\b{}\b".format(self.key), case = False)]
        
        # print(news_body_df['_source.body'])

        news_body = news_body_df.copy()
        news_body['filter_body'] = news_body['_source.body'].apply(DataPreproc.sentFilter)

        # print(self.news_body['filter_body'])

        news_body['tag_body'] = news_body['filter_body'].apply(TopicAlgo.lemma_tag)
        self.news_topics = news_body[['tag_body']]

        self.news_topics = self.news_topics[self.news_topics.astype(str)['tag_body'] != '[[]]']

        self.news_topics['topic'] , self.news_topics['keywords'] = zip(*self.news_topics['tag_body'].apply(TopicAlgo.get_topics))

    
    def topicResultDisplay(self):

        self.news_topics['text'] = self.news_topics['keywords'].apply(lambda x : ' '.join(x))

        top_keys = self.news_topics['topic'].apply(lambda x : list(x.keys())[0:3])
        df = top_keys.to_frame()

        y = df['topic'].value_counts().keys()[0:10]

        topw_lst = []
        for ix in range(0,len(list(y))):
            if (ix+1) > len(list(y))-1:
                continue
            jc = (len(set(list(y)[ix]) & set(list(y)[ix+1]))/float(len(set(list(y)[ix]) | set(list(y)[ix+1]))))*100
            
            if jc > 80.0:
                topw_lst.extend(list(y)[ix])

            if jc < 80.0:
                topw_lst.extend(list(y)[ix])
        
        key_set = list(set(topw_lst))

        article_perc = round(self.news_topics[self.news_topics['text'].str.contains('|'.join(key_set))]['text'].count()/ len(self.news_topics)*100)
        # popularity_perc = round(sum(list(self.news_body[self.news_body['text'].str.contains(' '.join(key_set))]['topic'].tolist()[0].values()))*100)

        return(key_set, article_perc) 
    
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-k', '--key', help = 'Enter the key', type = str)
    parser.add_argument('-t', '--time', help = 'Enter the time', type = str)
    
    # name = 'faircent'
    # era = 'pre'

    result = parser.parse_args()
    name = result.key
    era = result.time

    # path = 'C:/Users/HP/Pictures/wizikey_data_file/elastic_search_extract'
    path = 'C:/Users/HP/Pictures/wizikey_data_file/'

    # text_file = path + '/' + 'urgent' + '/' + f"{name}_{era}.csv"
    
    text_file = path + f"{name}.csv"
    # text_file = path + f"{name}.xlsx"
    # text_file = path + '/' + 'VC' + '/' + f"{name}.xlsx"
    
    df_articles = pd.read_csv(text_file)
    # df_articles = pd.read_excel(text_file)
    # df_articles = df_all

    obj = BaseClass(text_body = df_articles)

    topic_result, perc_coverage = obj.topicResultDisplay()

    print('\n')
    print(f"Topics For : {name}")
    print('\n')
    print("Top Trending Keywords  : {}".format(topic_result))
    print('\n')
    print("Percentage of Coverage (Popularity Score) : {} %".format(perc_coverage))


