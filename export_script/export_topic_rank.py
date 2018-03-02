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


def export_topic_twitter_rank(db="EsUserPost",collection="es_user_post"):
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    # docs = list()
    topic_list = []
    for doc in collections.find({"site":"tw"},{"entities.hashtags":1}):
        # print(doc)
        topic_list.extend(doc['entities']['hashtags'])

    myset = set(topic_list)  # myset是另外一个列表，里面的内容是mylist里面的无重复 项
    rank_data ={}
    for item in myset:
        print(item)
        rank_data[item]= topic_list.count(item)
        # print("the %s has found %d" % (item, topic_list.count(item)))
    result = sorted(rank_data.items(), key=lambda x: x[1], reverse=True)
    rank = [{'话题':it[0],'数量':it[1]} for it in result]
    print(rank)
    df2 = pd.DataFrame(rank)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                       decode('utf-8') if isinstance(x, str) else x)
    # print(docs)
    df2.to_excel('./export_data/%s/cipintongji/%s.xlsx' % ("twitter", 'topic_rank_2017'), sheet_name='Sheet1')


def export_topic_facebook_rank(db="EsUserPost",collection="es_user_post"):
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    docs = list(collections.find({"site":"fb"}))
    topic_list = []
    for doc in docs:
        topick_pre = list(map(lambda x: x.replace('#', ''), re.findall(r'#\s\S+|#\S+', doc['text'])))
        topick = list(map(lambda x: re.sub(
            r"[\u4E00-\u9FA5]|[\u3040-\u30FF\u31F0-\u31FF]|[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7AF]|\s|[-,.?:;\'\"!`]|(-{2})|(\.{3})|(\(\))|(\[\])|({}) ",
            '', x), topick_pre))
        topic_list.extend(topick)

    myset = set(topic_list)  # myset是另外一个列表，里面的内容是mylist里面的无重复 项
    rank_data ={}
    for item in myset:
        print(item)
        rank_data[item]= topic_list.count(item)
        # print("the %s has found %d" % (item, topic_list.count(item)))
    result = sorted(rank_data.items(), key=lambda x: x[1], reverse=True)
    rank = [{'话题':it[0],'数量':it[1]} for it in result]
    print(rank)
    df2 = pd.DataFrame(rank)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                       decode('utf-8') if isinstance(x, str) else x)
    # print(docs)
    df2.to_excel('./export_data/%s/cipintongji/%s.xlsx' % ("facebook", 'topic_rank_2017'), sheet_name='Sheet1')

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


def export_tmp():
    with open('facebook_user_ids.json','r') as f:
        ids=json.load(f)
    print(len(set(ids['ids'])))

export_tmp()
# export_topic_facebook_rank()
#export_topic_twitter_rank()