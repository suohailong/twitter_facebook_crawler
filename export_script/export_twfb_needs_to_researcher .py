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
import re,os
from pyquery import PyQuery as pq
from datetime import datetime
import asyncio
from aiohttp import ClientSession
import aiohttp
from bson import objectid


pp = pprint.PrettyPrinter(indent=4)


def asynchronous_request_facebook_api(ops=[]):
    try:
        async def request(url='',formdata={}):
            print('request ===>: %s ' % url)
            async with ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
                # session.keep_alive=False
                try:
                    async with session.post(url, proxy="http://127.0.0.1:51545",
                                           data={"text":formdata,"language":'en'},
                                           headers={'CONNECTION': 'close'}) as response:
                        result = await response.text()
                        # print(response.headers)
                        return result
                except Exception as e:
                    return None
                    # print(response)

        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        tasks = [
            asyncio.ensure_future(request(url=item['url'],formdata=item['data'])) for item in ops
        ]
        # tasks = [hello('https://www.facebook.com/'), hello('https://www.facebook.com/')]
        loop.run_until_complete(asyncio.wait(tasks))
        return [json.loads(task.result()) for task in tasks]
    # loop.close()
    except Exception as e:
      print(e)


#导出twitter用户的所有文章
def TwitterExportDataFromMongoToXlsx(db="UserPost",collection="user_post",project={}):
    #"mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin"
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    # db_user = client['Twitter']
    # collect_user = db_user['twitter']
    pipline = [
        {"$match": {
            "site": "twitter"
        }},
        {"$group": {
            "_id": "$user.id_str",
            "count": {"$sum": 1}
        }}
    ]
    result = list(collections.aggregate(pipline))
    # with open(os.path.abspath('./twitter_user_ids.json'), 'r') as f:
    #     user_ids = json.load(f)

    for id in list(map(lambda x: x['_id'], result)):
       # print(item['_id'])
       docs = list(collections.find({
           "user.id_str":id,
           "site":'twitter'
       }))
       formatDocs = []
       if(len(docs)>0):
           for doc in docs:
               d = pq(doc['source'])
               # print(doc)

               formatDocs.append({
                   'created_at':doc['created_at'],
                   'id': doc['id_str'],
                   'text': doc['text'],
                   'hot_topic': str(list(map(lambda x:x['text'] ,doc['entities']['hashtags'])) if len(doc['entities']['hashtags'])>0 else []),
                   'url_inside_tweet':str(list(map(lambda x:x['url'],doc['entities']['urls'])) if len(doc['entities']['urls'])>0 else []),
                       # {'hashtags': [{'text': 'NDAA', 'indices': [49, 54]}],'urls': [{'url': 'https://t.co/amE1rIBj0q', 'expanded_url': 'https://twitter.com/i/web/status/940632408382111745', 'display_url': 'twitter.com/i/web/status/9…', 'indices': [121, 144]}]},
                   'source': d('a').text(),
                       #'<a href="http://twitter.com" rel="nofollow">Twitter Web Client</a>',
                   'user_name': doc['user']['name'],
                   'link':'https://twitter.com/%s/status/%s' % (doc['user']['screen_name'],doc['id_str']),
                   'user_screen_name':doc['user']['screen_name'],
                   'geo': doc['geo'],
                   'coordinates': doc['coordinates'],
                   'place': doc['place'],
                   'contributors': doc['contributors'],
                   'retweet_count': doc['retweet_count'],
                   'favorite_count': doc['favorite_count'],
                   'reply_count': doc['replay_count'] if 'replay_count' in doc else  doc['reply_count']
               })
               # print(formatDocs[0])
           df2 = pd.DataFrame(formatDocs)
           df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                                           decode('utf-8') if isinstance(x, str) else x)
           # print(docs)
           df2.to_excel('./export_data/%s/UserTweets/%s.xlsx' % ("twitter",formatDocs[0]['user_name']), sheet_name='Sheet1')
       else:
           print(id)
       # break;

#name,id,current_location,birthday,category,fan_count,emails,hometown,link,location,website,likes.limit(3),new_like_count,about,description

#导出facebook用户的所有文章
def FacebookExportDataFromMongoToXlsx(db="UserPost",collection="user_post",project={}):
    #"mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin"
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    db_user = client['FaceBook']
    collect_user = db_user['facebook']

    with open(os.path.abspath('./facebook_user_ids.json'), 'r') as f:
        user_ids = json.load(f)

    for id in user_ids['ids']:
        user_account = collect_user.find_one({"id": id})
        docs = list(collections.find({
            "$or":[
                {"site": "facebook", "name": {"$regex": user_account['name']}},
                {"site": "facebook", "name": user_account['name']},
                {"site": "facebook", "name": {"$regex":user_account["keywords"]}}
            ]
        }))
        formatDocs = []
        # print(docs)
        for doc in docs:
           formatDocs.append({
               "id": str(doc['_id']),
               # "link": doc['link'] if 'link' in doc else '',
               "created_time": doc['create_at'],
               "text":doc["message"] if "message" in doc else '',
               # "description": doc['description'] if 'description' in doc else '',
               "user_name": doc['name'],
               # "user_id":doc['from']['id'],
               # "updated_time": doc["updated_time"],
               "share_num": doc['share_count'] if 'share_count' in doc else doc['share_num'],
               # "message_tags": str(doc['message_tags']) if 'message_tags' in doc else [],
               # "title": doc['name'] if 'name' in doc else '' ,
               "url": 'https://facebook.com%s' % doc["permalink_url"] if "permalink_url" in doc else '',
               "comment_num": doc["comment_num"] if 'comment_num' in doc else '',
               "likes_num": doc["likes_num"] if 'likes_num' in doc else '',
               "tags":list(map(lambda x:x.replace('#',''),re.findall(r'#\s\S+|#\S+',doc['message'])))

               # "share_num": doc["reactions"]['total_count'] if 'reactions' in doc else '',
           })
        df2 = pd.DataFrame(formatDocs)
        df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                                       decode('utf-8') if isinstance(x, str) else x)
        # print(docs)
        df2.to_excel('./export_data/%s/userPosts/%s.xlsx' % ("facebook", user_account['name']), sheet_name='Sheet1')

#鉴别出twitter未爬取到的用户
def TwitterDistUndevelopedIds(db='UserPost',collection="user_post"):
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    pipline = [
        {"$match": {
            "site": "twitter"
        }},
        {"$group": {
            "_id": "$user.id_str",
            "count": {"$sum": 1}
        }}
    ]
    result = list(collections.aggregate(pipline))
    with open('twitter_user_ids.json', 'r') as f:
        data = f.read()
        ids = json.loads(data)['ids']
    dbids = list(map(lambda x: x['_id'], result))
    print(len(set(ids)))
    tmp = []
    for id in dbids:
        if id in ids:
            continue;
        else:
            tmp.append(id)

    twitter_ids={}
    twitter_ids['ids'] = tmp;

    # print(len(tmp))
    with open('./twitter_weipa.json', 'w') as f:
        json.dump(twitter_ids, f, ensure_ascii=False, indent=4)


#鉴别出facebook未爬取到的用户
def FacebookDistUndevelopedIds(db='UserPost',collection="user_post"):
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    db_user = client['FaceBook']
    collect_user = db_user['facebook']

    with open(os.path.abspath('./facebook_user_ids.json'), 'r') as f:
        user_ids = json.load(f)

    wepa=[]
    for id in user_ids['ids']:
        # print(user_ids['ids'].index(id))
        user_account = collect_user.find_one({"id": id})
        doc = collections.find_one({
            "$or":[
                {"site": "facebook", "name": {"$regex": user_account['name']}},
                {"site": "facebook", "name": user_account['name']},
                {"site": "facebook", "name": {"$regex":user_account["keywords"]}}
            ]
        })
        # print(doc)
        if not doc:
            print(id)
            wepa.append(id)
    # print(wepa)
    facebook_ids = {}
    facebook_ids['ids'] = wepa;
    with open('./twitter_weipa.json', 'w') as f:
        json.dump(facebook_ids, f, ensure_ascii=False, indent=4)

#twitter用户各项指标计算
def export_indicator_twitter(db='UserPost',collection='user_post'):
    client = MongoClient()
    db_tweets = client['%s' % db]
    collect_tweets = db_tweets['%s' % collection]
    db_user = client['Twitter']
    collect_user = db_user['twitter']

    #根据现有的文章提取出用户
    pipline = [
        {"$match": {
            "site": "twitter"
        }},
        {"$group": {
            "_id": "$user.id_str",
            "count": {"$sum": 1}
        }}
    ]
    result = list(collect_tweets.aggregate(pipline))
    formatDocs = []
    for id in list(map(lambda x: x['_id'], result)):
        # 查找该永和的用户信息
        user_for_id = collect_user.find_one({'id_str':id})
        #查找该用户下的所有文章
        user_for_id_tweets_count = collect_tweets.count({"user.id_str": id,"site": 'twitter'})
        # print(user_for_id_tweets_count)
        if(user_for_id_tweets_count>0):
            aggregate_for_user_tweets = collect_tweets.aggregate([
                {
                  "$match":{
                      "user.id_str": id,
                      "site": 'twitter'
                  }
                },
                {"$group": {
                    "_id": "$user.id_str",
                    "count": {"$sum": 1},
                    "total_retweet_count":{"$sum":'$retweet_count'},
                    "total_replay_count":{"$sum":'$replay_count'},
                    "total_like_count":{"$sum":'$favorite_count'},
                    "avg_retweet_count":{"$avg":'$retweet_count'},
                    "avg_replay_count":{"$avg":'$replay_count'},
                    "avg_like_count": {"$avg": '$favorite_count'},
                    "max_retweet_count":{"$max":'$retweet_count'},
                    "max_replay_count":{"$max":'$replay_count'},
                    "max_like_count": {"$max": '$favorite_count'},
                }}
            ])

            user_tweet_summuary = list(aggregate_for_user_tweets)[0]
            user_summuary = {
                "名字":user_for_id['name'],
                '用户名':user_for_id['screen_name'],
                '用户粉丝量':user_for_id['followers_count'],
                '用户朋友数':user_for_id['friends_count'],
                "用户列表量":user_for_id['list_num'] if 'list_num' in user_for_id else 0,
                "用户瞬间量":user_for_id['moment_num'] if 'moment_num' in user_for_id else 0,
                "用户总推文数":user_for_id['statuses_count'],
                '季度总发帖量':user_tweet_summuary['count'],
                '被like数':user_for_id['favourites_count'],
                '总转发量':user_tweet_summuary['total_retweet_count'],
                '平均转发量':user_tweet_summuary['avg_retweet_count'],
                '单篇最大转发量':user_tweet_summuary['max_retweet_count'],
                '总评论量':user_tweet_summuary['total_replay_count'],
                '平均评论量':user_tweet_summuary['avg_replay_count'],
                '单篇最大评论量':user_tweet_summuary['max_replay_count'],
                '总点赞量':user_tweet_summuary['total_like_count'],
                '平均点赞量':user_tweet_summuary['avg_like_count'],
                '单篇最大点赞量':user_tweet_summuary['max_like_count']
            }
            print(user_summuary)
            formatDocs.append(user_summuary)
            print(len(formatDocs))
        else:
            print(id)
    df2 = pd.DataFrame(formatDocs)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                       decode('utf-8') if isinstance(x, str) else x)
    # print(docs)
    df2.to_excel('./export_data/%s/user_summary/%s.xlsx' % ("twitter","twitter_user_summary"),
                 sheet_name='Sheet1')

#facebook用户各项指标计算
def export_indicator_facebook(db='UserPost',collection='user_post'):
    client = MongoClient()
    db_tweets = client['%s' % db]
    collect_tweets = db_tweets['%s' % collection]
    db_user = client['FaceBook']
    collect_user = db_user['facebook']

    with open(os.path.abspath('./facebook_user_ids.json'), 'r') as f:
        user_ids = json.load(f)
    formatDocs=[]
    for id in user_ids['ids']:
        # 查找该永和的用户信息
        user_for_id = collect_user.find_one({"id": id})
        # 查找该用户下的所有文章
        user_for_id_tweets_count = collect_tweets.count({"$or":[
                {"site": "facebook", "name": {"$regex": user_for_id['name']}},
                {"site": "facebook", "name": user_for_id['name']},
                {"site": "facebook", "name": {"$regex":user_for_id["keywords"]}}
            ]})
        # print(user_for_id_tweets_count)
        if (user_for_id_tweets_count > 0):
            aggregate_for_user_tweets = collect_tweets.aggregate([
                {
                    "$match": {
                        "$or":[
                            {"site": "facebook", "name": {"$regex": user_for_id['name']}},
                            {"site": "facebook", "name": user_for_id['name']},
                            {"site": "facebook", "name": {"$regex":user_for_id["keywords"]}}
                        ]
                    }
                },
                {"$group": {
                    "_id": "$user.id_str",
                    "count": {"$sum": 1},
                    "total_retweet_count": {"$sum": '$share_count'},
                    "total_replay_count": {"$sum": '$comment_num'},
                    "total_like_count": {"$sum": '$likes_num'},
                    "avg_retweet_count": {"$avg": '$share_count'},
                    "avg_replay_count": {"$avg": '$comment_num'},
                    "avg_like_count": {"$avg": '$likes_num'},
                    "max_retweet_count": {"$max": '$share_count'},
                    "max_replay_count": {"$max": '$comment_num'},
                    "max_like_count": {"$max": '$likes_num'},
                }}
            ])

            user_tweet_summuary = list(aggregate_for_user_tweets)[0]
            user_summuary = {
                "名字": user_for_id['name'],
                '用户名': user_for_id['link'].split('/')[-1],
                '用户粉丝量': user_for_id['fan_count'],
                '季度总发帖量': user_tweet_summuary['count'],
                '被like数': user_for_id['likes_num'],
                '总转发量': user_tweet_summuary['total_retweet_count'],
                '平均转发量': user_tweet_summuary['avg_retweet_count'],
                '单篇最大转发量': user_tweet_summuary['max_retweet_count'],
                '总评论量': user_tweet_summuary['total_replay_count'],
                '平均评论量': user_tweet_summuary['avg_replay_count'],
                '单篇最大评论量': user_tweet_summuary['max_replay_count'],
                '总点赞量': user_tweet_summuary['total_like_count'],
                '平均点赞量': user_tweet_summuary['avg_like_count'],
                '单篇最大点赞量': user_tweet_summuary['max_like_count']
            }
            print(user_summuary)
            formatDocs.append(user_summuary)
            print(len(formatDocs))
        else:
            print(id)
    df2 = pd.DataFrame(formatDocs)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                       decode('utf-8') if isinstance(x, str) else x)
    # print(docs)
    df2.to_excel('./export_data/%s/user_summary/%s.xlsx' % ("facebook", 'facebook_user_summary'),
                 sheet_name='Sheet1')



def export_twitterUser_emotion_analysis(db='UserPost',collection="user_post"):
    client = MongoClient()
    db_tweets = client['%s' % db]
    collect_tweets = db_tweets['%s' % collection]
    db_user = client['Twitter']
    collect_user = db_user['twitter']
    from funcy import flatten,concat,group_by
    # 根据现有的文章提取出用户
    pipline = [
        {"$match": {
            "site": "twitter"
        }},
        {"$group": {
            "_id": "$user.id_str",
            "count": {"$sum": 1}
        }}
    ]
    result = list(collect_tweets.aggregate(pipline))
    formatDocs = []
    for id in list(map(lambda x: x['_id'], result)):
        # 查找该永和的用户信息
        user_for_id = collect_user.find_one({'id_str': id})
        # 查找该用户下的所有文章
        user_for_id_tweets_count = collect_tweets.count({"user.id_str": id, "site": 'twitter'})
        # print(user_for_id_tweets_count)
        if (user_for_id_tweets_count > 0):
            aggregate_for_user_tweets = collect_tweets.aggregate([
                {
                    "$match": {
                        "user.id_str": id,
                        "site": 'twitter'
                    }
                },
                {"$group": {
                    "_id": "$user.id_str",
                    "text":{"$push":"$text"}
                }}
            ])

            user_tweets_texts = list(aggregate_for_user_tweets)[0]
            # print(len(user_tweets_texts['text']))

            # print(texts)
            if len(user_tweets_texts['text'])>300:
                ops = [{'url':'https://tone-analyzer-demo.ng.bluemix.net/api/tone','data':''.join(user_tweets_texts['text'][i:i+300])} for i in range(0,len(user_tweets_texts['text']),300)]
            else:
                texts = ''.join(user_tweets_texts['text'])
                ops = [{'url':'https://tone-analyzer-demo.ng.bluemix.net/api/tone','data':texts}]
            # print(ops)
            analyzer = asynchronous_request_facebook_api(ops)
            # print(analyzer[0])
            final_result  = list(concat(list(flatten(list(map(lambda x:x['document_tone']['tones'],analyzer))))))
            group_result = group_by(lambda x:x['tone_name'],final_result)
            
            formatDocs.append({})
            print(len(formatDocs))
        else:
            print(id)
    df2 = pd.DataFrame(formatDocs)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                       decode('utf-8') if isinstance(x, str) else x)
    # print(docs)
    df2.to_excel('./export_data/%s/user_summary/%s.xlsx' % ("twitter", "twitter_user_summary"),
                 sheet_name='Sheet1')

def tmp_export():
    client = MongoClient('mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin')
    userdb = client['FaceBook']
    user = userdb['facebook']

    for user_data in user.find({}):
        if not user_data['link'].endswith('/'):
            user_data['link'] = user_data['link']+'/'
            print(user_data['_id'])
            # update_status = user.update({'_id':user_data['_id']},user_data)
            # print(update_status)




tmp_export()
# TwitterDistUndevelopedIds()
# FacebookDistUndevelopedIds()
# TwitterExportDataFromMongoToXlsx()
# FacebookExportDataFromMongoToXlsx()
#export_indicator_facebook()
