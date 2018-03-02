from pymongo import MongoClient,DESCENDING,ReturnDocument
import openpyxl
from openpyxl.workbook import Workbook
from openpyxl.writer.excel import ExcelWriter
import openpyxl.cell
from openpyxl.reader.excel import load_workbook
import pandas as pd
import numpy as np
import pprint
import json,os
import re
from pyquery import PyQuery as pq
from datetime import datetime
from funcy import sums

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

#导出facebook twitter地区排名
def export_Area_rank(db="EsUserPost",collection="es_user_post",filename='area',sheet='Sheet1',index='8',export_filename='Area'):
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]

    items = pd.read_excel('./%s.xlsx' % filename, sheet, index_col=None, na_values=['NA'])
    df = pd.DataFrame(items)
    keyWordItems = df.values.tolist()[0:]
    twitter_rank = {}
    facebook_rank = {}
    # print(keyWordItems)
    # list(set([item[1] for item in keyWordItems]))
    for item in list(set([item[index] for item in keyWordItems])):
        # print(item)
        twitter_numbers = collections.count({"site":"tw",'text':{"$regex":item}})
        facebook_numbers = collections.count({"site":'fb','text':{"$regex":item}})
        twitter_rank[item]=twitter_numbers,
        facebook_rank[item] = facebook_numbers,
        print(item,twitter_numbers,facebook_numbers)

    twitter_result = sorted(twitter_rank.items(), key=lambda x: x[1], reverse=True)
    facebook_result = sorted(facebook_rank.items(), key=lambda x: x[1], reverse=True)


    twitter_final_rank = [{'地区/国家':it[0],'数量':it[1]} for it in twitter_result]
    facebook__final_rank = [{'地区/国家': it[0], '数量': it[1]} for it in facebook_result]

    write_excel('./export_data/%s/contry/%s.xlsx' % ("twitter", 'twitter_%s_rank_2017' % export_filename),twitter_final_rank)
    write_excel('./export_data/%s/contry/%s.xlsx' % ("facebook", 'facebook_%s_rank_2017' % export_filename), facebook__final_rank)
# export_Area_rank()

#导出twitter@用户名排名
def export_twitter_name_rank(db="EsUserPost",collection="es_user_post"):
    collections = mongo_login(db,collection)
    twitter = mongo_login('Twitter','twitter')
    pipline = [
        {"$match": {
            "site": "tw"
        }},
        {"$group": {
            "_id": "$user.id_str",
            "count": {"$sum": 1}
        }}
    ]
    result = collections.aggregate(pipline)
    result = list(map(lambda x:x['_id'],result))
    twitter_rank = {}
    for item in result:
        print(item)
        doc = twitter.find_one({"id_str":item})
        if doc:
            text = '@'+doc['screen_name']
            # text = '@'+link.split('/')[3]
            print(text)
            twitter_numbers = collections.count({"site":"tw",'text':{"$regex":text}})
            # facebook_numbers = collections.count({"site":{"$ne":True},'message':{"$exists":True},'message':{"$regex":text}})
            twitter_rank[text[1:]]=twitter_numbers,
            # facebook_rank[text[1:]] = facebook_numbers,
            print(text[1:],twitter_numbers)
        else:
            print(doc)
        # print(item,twitter_numbers,facebook_numbers)
    twitter_result = sorted(twitter_rank.items(), key=lambda x: x[1], reverse=True)
    # facebook_result = sorted(facebook_rank.items(), key=lambda x: x[1], reverse=True)
    twitter_final_rank = [{'地区/国家':it[0],'数量':it[1]} for it in twitter_result]
    write_excel('./export_data/%s/contry/%s.xlsx' % ("twitter", 'twitter_name_rank_2017'),twitter_final_rank)


#导出facebook@用户名排名
def export_facebook_name_rank(db="EsUserPost",collection="es_user_post"):
    collections = mongo_login(db,collection)
    twitter = mongo_login('FaceBook','facebook')
    with open(os.path.abspath('./facebook_user_ids.json'), 'r') as f:
        user_ids = json.load(f)
    facebook_rank = {}
    for id in user_ids['ids']:
        doc = twitter.find_one({"id":id})
        if doc:
            text = '@'+doc['link'].split('/')[3]
            print(text)
            #print(collections.find_one({"site": 'facebook', 'message': {"$regex": text}},{'message':1}))
            facebook_numbers= collections.count({"site":'fb','text':{"$regex":text}})
            facebook_rank[text[1:]] = facebook_numbers,
            print(text[1:],facebook_numbers,facebook_rank[text[1:]])
        else:
            print(doc)
    facebook_result = sorted(facebook_rank.items(), key=lambda x: x[1], reverse=True)
    facebook__final_rank = [{'用户名': it[0], '数量': it[1][0]} for it in facebook_result]

    write_excel('./export_data/%s/contry/%s.xlsx' % ("facebook", 'facebook_name_rank_2017'),facebook__final_rank)


#导出twitter平均年龄等
def export_twitter_age_stages_summary(db="EsUserPost",collection="es_user_post"):
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]

    client2 = MongoClient("mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin")
    tw_db = client2['Twitter']
    tw_user = tw_db['twitter']

    fb_db = client2['FaceBook']
    fb_user = fb_db['facebook']

    #导入用户年龄字段
    # items = pd.read_excel('./Us_user.xlsx', 'others', index_col=None, na_values=['NA'])
    # df = pd.DataFrame(items)
    # keyWordItems = df.values.tolist()[0:]
    # print(keyWordItems)
    # for item in keyWordItems:
    #     print({"keywords":item[1]},{'$set':{'user_age':item[5]}})
    #     tw=users.update_many({"keywords":item[1]},{'$set':{'user_age':item[5]}})
    #     fb=fb_user.update_many({"keywords": item[1]}, {'$set': {'user_age': item[5]}})
    #     print(tw.modified_count,fb.modified_count)

    # #导入正确用户字段
    with open(os.path.abspath('facebook_user_ids.json'), 'r') as f:
        usr_ids = json.load(f)
    for id in usr_ids['ids']:
        tw_update = fb_user.find_one_and_update({"id":id}, {
                            '$set': {'monitor':True}
                        },return_document=ReturnDocument.AFTER)
        print('更新了%s个' % tw_update['_id'])
    print(len(usr_ids))
    formatDocs = []
    # for i in range(0,60,10):
    #     doc = tw_user.find({'monitor':True,'age':{'$lte':39+i,'$gte':30+i}})
    #     user_retweet=[]
    #     user_replay=[]
    #     user_like=[]
    #     user_year=[]
    #     user_fawen=[]
    #     user_summary = {}
    #     for user_item in doc:
    #         # print(type(user_item['id_str']))
    #         # print(list(collections.find({'site':'tw','user.id_str':user_item['id_str']})))
    #         user_tweet = collections.aggregate([
    #             {
    #                 "$match": {
    #                     "keywords": user_item['id_str'],
    #                     "site": 'tw'
    #                 }
    #             },
    #             {"$group": {
    #                 "_id": "$keywords",
    #                 "count":{"$sum": 1},
    #                 "total_retweet_count": {"$sum": '$retweet_count'},
    #                 "total_replay_count": {"$sum": '$reply_count'},
    #                 "total_like_count": {"$sum": '$favorite_count'},
    #             }}
    #         ])
    #         # print(list(user_tweet))
    #         tian_tweets = list(user_tweet)
    #         if len(tian_tweets)==0:
    #             continue
    #         user_tweets = list(tian_tweets).pop()
    #         user_replay.append(user_tweets['total_replay_count'])
    #         user_retweet.append(user_tweets['total_retweet_count'])
    #         user_like.append(user_tweets['total_like_count'])
    #         # print(item['created_at'])
    #         time_str = datetime.strptime(user_item['created_at'], '%a %b %d %H:%M:%S %z %Y').year
    #         print(time_str)
    #         user_year.append(2017-time_str)
    #         user_fawen.append(user_tweets['count'])
    #         #print(user_tweets['total_replay_count'],user_tweets['total_retweet_count'],user_tweets['total_like_count'])
    #     user_summary['年龄段'] = '%s-%s' % (30+i,39+i)
    #     user_summary['平均使用年限']=sum(user_year)/len(user_year)
    #     user_summary['发文量'] = sum(user_fawen) / len(user_fawen)
    #     user_summary['平均转发'] = sum(user_retweet)/len(user_retweet)
    #     user_summary['平均评论']=sum(user_replay)/len(user_replay)
    #     user_summary['平均点赞']=sum(user_like)/len(user_like)
    #     formatDocs.append(user_summary)
    # df2 = pd.DataFrame(formatDocs)
    # df2 = df2.applymap(lambda x: x.encode('unicode_escape').
    #                    decode('utf-8') if isinstance(x, str) else x)
    # # print(docs)
    # df2.to_excel('./export_data/%s/user_summary/%s.xlsx' % ("twitter", "twitter_year_summary_2017"),
    #              sheet_name='Sheet1')

#导出facebook平均年龄等
def export_facebook_age_stages_summary(db="EsUserPost",collection="es_user_post"):
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]

    client2 = MongoClient("mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin")
    tw_db = client2['Twitter']
    tw_user = tw_db['twitter']

    fb_db = client2['FaceBook']
    fb_user = fb_db['facebook']
    formatDocs = []
    for i in range(0,60,10):
        doc = fb_user.find({'monitor':True,'age':{'$lte':39+i,'$gte':30+i}})

        user_retweet=[]
        user_replay=[]
        user_like=[]
        user_year=[]
        user_fawen = []
        user_summary = {}
        for user_item in doc:
            user_tweet = collections.aggregate([
                {
                    "$match": {
                        "user.id": user_item['id'],
                        'site':'fb'
                    }
                },
                {"$group": {
                    "_id": "$user.id",
                    "count": {"$sum": 1},
                    "total_retweet_count": {"$sum": '$share_count'},
                    "total_replay_count": {"$sum": '$comment_num'},
                    "total_like_count": {"$sum": '$likes_num'},
                }}
            ])
            #print(list(user_tweet))
            tian_tweets = list(user_tweet)
            if len(tian_tweets)==0:
                continue
            user_tweets = list(tian_tweets).pop()
            user_replay.append(user_tweets['total_replay_count'])
            user_retweet.append(user_tweets['total_retweet_count'])
            user_like.append(user_tweets['total_like_count'])
            user_fawen.append(user_tweets['count'])
            print(user_tweets['total_replay_count'],user_tweets['total_retweet_count'],user_tweets['total_like_count'])
        user_summary['年龄段'] = '%s-%s' % (30+i,39+i)
        user_summary['发文量'] = sum(user_fawen)/len(user_fawen)
        # user_summary['平均使用年限']=sum(user_year)/len(user_year)
        user_summary['平均转发'] = sum(user_retweet)/len(user_retweet)
        user_summary['平均评论']=sum(user_replay)/len(user_replay)
        user_summary['平均点赞']=sum(user_like)/len(user_like)
        formatDocs.append(user_summary)
    df2 = pd.DataFrame(formatDocs)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                       decode('utf-8') if isinstance(x, str) else x)
    # print(docs)
    df2.to_excel('./export_data/%s/user_summary/%s.xlsx' % ("facebook", "facebook_year_summary_2017"),
                 sheet_name='Sheet1')

#导出Twitter关注和被关注的关系
def export_twitter_flowing_reactions(db="EsUserPost",collection="es_user_post"):
    collections = mongo_login(db,collection)
    twitter = mongo_login('Twitter','twitter')
    pipline = [
        {"$match": {
            "site": "tw"
        }},
        {"$group": {
            "_id": "$user.id_str",
            "count": {"$sum": 1}
        }}
    ]
    result = collections.aggregate(pipline)
    result = list(map(lambda x:x['_id'],result))
    user_screen_name=[twitter.find_one({"id_str":item})['screen_name'] for item in result]
    twitter_apk=[]
    for item in result:
        # print(item)
        twitter_item_record={}
        doc = twitter.find_one({"id_str":item})
        if doc:
            text = '@'+doc['screen_name']
            flowing_users = collections.find({"site":"tw",'text':{"$regex":text}},{'user.screen_name':1})
            ni = list(flowing_users)
            if len(ni)==0:
                continue
            ta =list(map(lambda x:x['user']['screen_name'],ni))
            flowing_users_1 = list(set(ta))
            twitter_item_record['用户名']=doc['screen_name']
            for name in user_screen_name:
                twitter_item_record[name]=1 if name in ta else 0
            print(twitter_item_record)
            twitter_apk.append(twitter_item_record)
        else:
            print(doc)
    # twitter_final_rank = [{'地区/国家':it[0],'数量':it[1]} for it in twitter_result]
    write_excel('./export_data/%s/contry/%s.xlsx' % ("twitter", 'twitter_user_reactions_2017'),twitter_apk)

def export_facebook_flowing_reactions(db="UserPost",collection="user_post"):
    collections = mongo_login(db,collection)
    facebook = mongo_login('FaceBook','facebook')

    with open(os.path.abspath('./facebook_user_ids.json'), 'r') as f:
        user_ids = json.load(f)

    user_screen_name=[facebook.find_one({"id":id})['link'].split('/')[3] for id in user_ids['ids']]
    facebook_apk=[]
    for item in user_ids['ids']:
        print(item)
        facebook_item_record={}
        doc = facebook.find_one({"id":item})
        if doc:
            text = '@' + doc['link'].split('/')[3]
            flowing_users = collections.find({"site":'facebook','message':{"$regex":text}})
            ni = list(flowing_users)
            print(ni)
            if len(ni)==0:
                continue
            ta =list(map(lambda x:x['permalink_url'].split('/')[1],ni))
            flowing_users_1 = list(set(ta))
            facebook_item_record['用户名']=doc['link'].split('/')[3]
            for name in user_screen_name:
                facebook_item_record[name]=1 if name in ta else 0
            print(facebook_item_record)
            facebook_apk.append(facebook_item_record)
        else:
            print(doc)
    # twitter_final_rank = [{'地区/国家':it[0],'数量':it[1]} for it in twitter_result]
    write_excel('./export_data/%s/contry/%s.xlsx' % ("facebook", 'facebook_user_reactions'),facebook_apk)

# export_twitter_name_rank()
# export_facebook_name_rank()
export_Area_rank(filename='city',sheet='查询1',index=1,export_filename='state')
