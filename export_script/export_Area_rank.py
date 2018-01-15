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


def mongo_login(db,collection):
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    return collections

def write_excel(fname='./export_data/%s/contry/%s.xlsx' % ("twitter", 'twitter_Area_rank'),data=None):
    df2 = pd.DataFrame(data)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                       decode('utf-8') if isinstance(x, str) else x)
    # print(docs)
    df2.to_excel(fname, sheet_name='Sheet1')

def export_Area_rank(db="UserPost",collection="user_post"):
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]

    items = pd.read_excel('./city.xlsx', '查询1', index_col=None, na_values=['NA'])
    df = pd.DataFrame(items)
    keyWordItems = df.values.tolist()[0:]
    twitter_rank = {}
    facebook_rank = {}
    # print(keyWordItems)
    # list(set([item[1] for item in keyWordItems]))
    for item in list(set([item[1] for item in keyWordItems])):

        twitter_numbers = collections.count({"site":"twitter",'text':{"$regex":item}})
        facebook_numbers = collections.count({"site":{"$ne":True},'message':{"$exists":True},'message':{"$regex":item}})
        twitter_rank[item]=twitter_numbers,
        facebook_rank[item] = facebook_numbers,
        print(item,twitter_numbers,facebook_numbers)

    twitter_result = sorted(twitter_rank.items(), key=lambda x: x[1], reverse=True)
    facebook_result = sorted(facebook_rank.items(), key=lambda x: x[1], reverse=True)


    twitter_final_rank = [{'地区/国家':it[0],'数量':it[1]} for it in twitter_result]
    facebook__final_rank = [{'地区/国家': it[0], '数量': it[1]} for it in facebook_result]

    write_excel('./export_data/%s/contry/%s.xlsx' % ("twitter", 'twitter_State_rank'),twitter_final_rank)
    write_excel('./export_data/%s/contry/%s.xlsx' % ("facebook", 'facebook_State_rank'), facebook__final_rank)
# export_Area_rank()


def export_name_rank(db="UserPost",collection="user_post"):
    collections = mongo_login(db,collection)
    facebook = mongo_login('FaceBook','facebook')
    pipline = [
        {"$match": {
            "site": {"$ne":'twitter'}
        }},
        {"$group": {
            "_id": "$from.id",
            "count": {"$sum": 1}
        }}
    ]
    result = collections.aggregate(pipline)
    result = list(map(lambda x:x['_id'],result))


    # print(result)
    twitter_rank = {}
    facebook_rank = {}
    # print(keyWordItems)
    # list(set([item[1] for item in keyWordItems]))
    for item in result:

        doc = facebook.find_one({"id":item})
        if doc:
            text = '@'+doc['name']
            # text = '@'+link.split('/')[3]
            print(text)
            # twitter_numbers = collections.count({"site":"twitter",'text':{"$regex":text}})
            facebook_numbers = collections.count({"site":{"$ne":True},'message':{"$exists":True},'message':{"$regex":text}})
            # twitter_rank[text[1:]]=twitter_numbers,
            facebook_rank[text[1:]] = facebook_numbers,
            print(text[1:],facebook_numbers)
        else:
            print(doc)
        # print(item,twitter_numbers,facebook_numbers)

    # twitter_result = sorted(twitter_rank.items(), key=lambda x: x[1], reverse=True)
    facebook_result = sorted(facebook_rank.items(), key=lambda x: x[1], reverse=True)


    # twitter_final_rank = [{'地区/国家':it[0],'数量':it[1]} for it in twitter_result]
    facebook__final_rank = [{'用户名': it[0], '数量': it[1]} for it in facebook_result]

    # write_excel('./export_data/%s/contry/%s.xlsx' % ("twitter", 'twitter_name_rank'),twitter_final_rank)
    write_excel('./export_data/%s/contry/%s.xlsx' % ("facebook", 'facebook_name2_rank'), facebook__final_rank)
export_name_rank()