import numpy as np
import pandas as pd 
import requests
import json 
import argparse
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from data_preprocessing import DataPreproc
from topic_model_algo import TopicAlgo

class BaseClass:
    def __init__(self,**kwargs):
        if 'text_body' in kwargs:
            self.body = kwargs['text_body']
        if 'keyword' in kwargs:
            self.key = kwargs['keyword']
        
        news_body_df = pd.json_normalize(self.body)
        # print(news_body_df)
        print(self.key)
        news_body_df = news_body_df.dropna()
        news_body_df = news_body_df[news_body_df['body'].str.contains(r"\b{}\b".format(self.key), case = False)]
        # print(news_body_df)

        news_body = news_body_df.copy()
        news_body['filter_body'] = news_body['body'].apply(DataPreproc.sentFilter)

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

    parser.add_argument("-k", "--key", help = 'Enter the key',type = str, nargs = '+')
    parser.add_argument("-st", "--start_date", help = 'Enter the date of starting', type = str)
    parser.add_argument("-et", "--end_date", help = 'Enter the date of ending', type = str)

    result = parser.parse_args()

    _transport = RequestsHTTPTransport(url = 'http://staging.wizikey.com:4000/graphql/', use_json= True)

    client = Client(transport= _transport, fetch_schema_from_transport= True)

    query = """{
        articles(
            filters: {
                q : "%s",,
                timeFrom: "%sT08:57:44+0500"
                timeTo: "%sT18:29:59.999Z"
            }
            offset: %d,
            limit: %d
        ) {
            headline
            body
        }
    }"""
    
    data_lst = []
    kw = ' '.join(result.key)
    start_date = result.start_date 
    end_date = result.end_date
    limit = 1000
    # print(kw)
    # print(start_date)
    # print(end_date)
    for off_set in range(0,10000,limit):
        print(('*=*>') * int(off_set/limit))
        
        g_query = query%(kw,start_date,end_date,off_set,limit)
        # print(g_query)
        data_lst.extend(client.execute(gql(g_query))['articles'])
    # print(data_lst)
    obj = BaseClass(text_body = data_lst, keyword = kw)

    topic_result, perc_coverage = obj.topicResultDisplay()

    print('\n')
    print("Top Trending Keywords  : {}".format(topic_result))
    print('\n')
    print("Percentage of Coverage (Popularity Score) : {} %".format(perc_coverage))


