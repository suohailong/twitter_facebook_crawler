from pymongo import MongoClient,DESCENDING
import openpyxl
from openpyxl.workbook import Workbook
from openpyxl.writer.excel import ExcelWriter
import openpyxl.cell
from openpyxl.reader.excel import load_workbook
import pandas as pd
import numpy as np
import pprint
import json
import re
from pyquery import PyQuery as pq
from datetime import datetime

pp = pprint.PrettyPrinter(indent=4)


def export_topic_twitter_rank(db="UserPost",collection="user_post"):
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    docs = list(collections.find({"site":"twitter"},{"entities.hashtags":1}))
    topic_list = []
    for doc in docs:
        topic_list.extend(list(map(lambda x:x['text'],doc['entities']['hashtags'])) if len(doc['entities']['hashtags'])>0 else [])

    myset = set(topic_list)  # myset是另外一个列表，里面的内容是mylist里面的无重复 项
    rank_data ={}
    for item in myset:
        rank_data[item]= topic_list.count(item)
        # print("the %s has found %d" % (item, topic_list.count(item)))
    result = sorted(rank_data.items(), key=lambda x: x[1], reverse=True)
    rank = [{'话题':it[0],'数量':it[1]} for it in result]
    print(rank)
    df2 = pd.DataFrame(rank)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                       decode('utf-8') if isinstance(x, str) else x)
    # print(docs)
    df2.to_excel('./export_data/%s/cipintongji/%s.xlsx' % ("twitter", 'topic_rank'), sheet_name='Sheet1')


def export_topic_facebook_rank(db="UserPost",collection="user_post"):
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    docs = list(collections.find({"site":"facebook"}))
    topic_list = []
    for doc in docs:
        topic_list.extend(list(map(lambda x: x.replace('#', ''), re.findall(r'#\s\S+|#\S+', doc['message']))))

    myset = set(topic_list)  # myset是另外一个列表，里面的内容是mylist里面的无重复 项
    rank_data ={}
    for item in myset:
        rank_data[item]= topic_list.count(item)
        # print("the %s has found %d" % (item, topic_list.count(item)))
    result = sorted(rank_data.items(), key=lambda x: x[1], reverse=True)
    rank = [{'话题':it[0],'数量':it[1]} for it in result]
    print(rank)
    df2 = pd.DataFrame(rank)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                       decode('utf-8') if isinstance(x, str) else x)
    # print(docs)
    df2.to_excel('./export_data/%s/cipintongji/%s.xlsx' % ("facebook", 'topic_rank'), sheet_name='Sheet1')

def export_twitter_user_all_fileds():
    client = MongoClient()
    dbs = client['%s' % "Twitter"]
    collections = dbs['%s' % 'twitter']
    doc = collections.find_one({})
    a = []
    for k,v in doc.items():
        print(k)
        a.append(k)
    print(a)
export_twitter_user_all_fileds()