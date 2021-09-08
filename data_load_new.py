import numpy as np
import pandas as pd 
import requests
import json 
import argparse
import pickle
import plotly.graph_objects as go 
from sklearn import preprocessing
import xgboost as xgb
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from data_preprocessing import DataPreproc
from topic_model_algo import TopicAlgo

class BaseClass:
    def __init__(self,**kwargs):
        if 'text_body' in kwargs:
            self.body = kwargs['text_body']
        # if 'keyword' in kwargs:
        #     self.key = kwargs['keyword']
        
        news_body_df = pd.json_normalize(self.body)
        # print(news_body_df)
        # print(self.key)
        news_body_df = news_body_df.dropna()
        # news_body_df = news_body_df[news_body_df['body'].str.contains(r"\b{}\b".format(self.key))]
        # print(news_body_df)

        news_body = news_body_df.copy()
        # print(news_body['body'])

        news_body['filter_body'] = news_body['body'].apply(DataPreproc.sentFilter)

        # print(news_body['filter_body'])

        news_body['tag_body'] = news_body['filter_body'].apply(TopicAlgo.lemma_tag)
        self.news_topics = news_body[['tag_body']]

        self.news_topics = self.news_topics[self.news_topics.astype(str)['tag_body'] != '[[]]']

        self.news_topics['topic'] , self.news_topics['keywords'] = zip(*self.news_topics['tag_body'].apply(TopicAlgo.get_topics))

    
    def label_predic(self):

        self.news_topics['text'] = self.news_topics['keywords'].apply(lambda x : ' '.join(x))

        with open('C:\\Users\\HP\\Pictures\\xgb_model.txt', 'rb') as model_run:
            xgb_model = pickle.load(model_run)
        
        with open('C:\\Users\\HP\\Pictures\\vector.txt', 'rb') as vector:
            vectorizer = pickle.load(vector)
        
        input_data = vectorizer.transform(self.news_topics['text'])
        new_labels = xgb_model.predict(input_data)

        encoder = preprocessing.LabelEncoder()

        p = {'labels': encoder.inverse_transform(new_labels)}
        df_labels = pd.DataFrame(data = p)
        df_perc = round(df_labels['labels'].value_counts(normalize= True) *100,2).to_frame().reset_index().rename(columns = {'index' : 'name','labels' : 'perc'})

        return(df_perc)
    
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
                q : "*",,
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
    obj = BaseClass(text_body = data_lst)

    df_display = obj.label_predic()

    print('\n')
    print("Top Trending Keywords  : {}".format(df_display))


