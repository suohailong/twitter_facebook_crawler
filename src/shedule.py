from pymongo import MongoClient,DESCENDING,ReturnDocument
import openpyxl
from openpyxl.workbook import Workbook
from openpyxl.writer.excel import ExcelWriter
import openpyxl.cell
from openpyxl.reader.excel import load_workbook
import pandas as pd
import numpy as np
import pprint
import json,time
import re,os
from pyquery import PyQuery as pq
from datetime import datetime

def create_mongo_conn(mnStr='mongodb://127.0.0.1:27017',db="UserPost",collection="user_post"):
    print('connected==>%s' % mnStr)
    client = MongoClient(mnStr)
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    return collections

def crawler_user_listcounts(crawler):
    with open(os.path.abspath('twitter_user_ids.json'), 'r') as f:
        user_ids = json.load(f)
    for id in user_ids['ids']:
        db=create_mongo_conn(db='Twitter',collection='twitter')
        doc = db.find_one({"id_str":id})
        list_count = crawler.crawler_list_count(doc["screen_name"])
        print(id)
        db.find_one_and_update({"id_str":id},{"$set":{"list_num":list_count}})


def crawler_users(crawler, kwords, typeIndex):
    for kw in kwords:
        print('\n')
        print('--------start kw=>%s----------' % kw[int(typeIndex)])
        crawler.search_users(kw, typeIndex)
        print('----------end kw=>%s----------\n' % kw[int(typeIndex)])
        time.sleep(1)
    print('all kws is processed')




def crawler_tweets(crawler,site,filename):

    if site=='twitter':
        with open(os.path.abspath(filename), 'r') as f:
            user_ids = json.load(f)
        print(len(user_ids['ids']))
        for id in user_ids['ids']:
            crawler.fetch_user_tweets(user_id=id,deadline='2017-9-21')
    else:
        db = create_mongo_conn(db='FaceBook',collection='facebook')
        with open(os.path.abspath(filename), 'r') as f:
            user_ids = json.load(f)
        print(len(user_ids['ids']))
        for id in user_ids['ids']:
            doc = db.find_one({"id":id})

            crawler.fetch_user_tweets(id=id,urls=doc['link']+'posts/',deadline='2017-9-21')




def crawler_reactions(crawler):
    db = create_mongo_conn()
    tweets = db.find({"site": {"$ne": 'twitter'}, "id": {"$exists": True}, "likes_num": {"$exists": False}})
    for item in tweets:
        content = crawler.crawler_reactions_nums(item['permalink_url'])
        update_doc = db.update_many({"permalink_url": item['permalink_url']}, {
            '$set': {'comment_num': content[0]['reactions']['comment_count'],
                     'likes_num': content[0]['reactions']['likes_count'],
                     'share_count': content[0]['reactions']["share_count"], 'site': 'facebook'}
        })
        print(update_doc.modified_count)

def crawler_user_likes_counts(crawler):
    db = create_mongo_conn(db='FaceBook', collection='facebook')
    while True:
        tweets = list(db.find({"likes_num":0}).limit(20))
        if(len(tweets)==0):
            break;
        urls = map(lambda x:x['link'],tweets)
        content = crawler.crawler_user_likes(urls)

        for item in content:
            update_doc = db.update_many({"link": item['url']}, {
                '$set': {'likes_num': item['like_count']}
            })
            print(update_doc.modified_count)