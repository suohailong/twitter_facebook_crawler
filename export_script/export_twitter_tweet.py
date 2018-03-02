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
import funcy.py3 as funcy

pp = pprint.PrettyPrinter(indent=4)


def asynchronous_request(ops=[]):
    async def request(url=''):
        print('request ===>: %s ' % url)
        async with ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
            # session.keep_alive=False
            try:
                async with session.get(url,headers={'CONNECTION': 'close','CONTENT-TYPE': 'application/json'}) as response:
                    result = await response.text()
                    # print(response.headers)
                    # print(result)
                    return result
            except Exception as e:
               raise e

    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    tasks = [
        asyncio.ensure_future(request(url=item['url'])) for item in ops
    ]
    # tasks = [hello('https://www.facebook.com/'), hello('https://www.facebook.com/')]
    loop.run_until_complete(asyncio.wait(tasks))
    return [json.loads(task.result()) for task in tasks]



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
def TwitterDistUndevelopedIds(db='TUserPost',collection="Tuser_post"):
    client = MongoClient('mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin')
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
    tmp = []
    for id in dbids:
        if id in ids:
            # print(id)
            continue;
        else:
            print(id)
            tmp.append(id)

    twitter_ids={}
    twitter_ids['ids'] = tmp;

    # print(len(tmp))
    with open('./twitter_weipa.json', 'w') as f:
        json.dump(twitter_ids, f, ensure_ascii=False, indent=4)


#鉴别出facebook未爬取到的用户
def FacebookDistUndevelopedIds(db='TUserPost',collection="Tuser_post"):
    client = MongoClient('mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin')
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
def export_indicator_twitter(db='EsUserPost',collection='es_user_post'):
    client = MongoClient()
    db_tweets = client['%s' % db]
    collect_tweets = db_tweets['%s' % collection]
    client2 = MongoClient("mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin")
    db_user = client2['Twitter']
    collect_user = db_user['twitter']

    #根据现有的文章提取出用户
    pipline = [
        {"$match": {
            "site": "tw"
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
        user_for_id_tweets_count = collect_tweets.count({"user.id_str": id,"site": 'tw'})
        # print(user_for_id_tweets_count)
        if(user_for_id_tweets_count>0):
            aggregate_for_user_tweets = collect_tweets.aggregate([
                {
                  "$match":{
                      "user.id_str": id,
                      "site": 'tw'
                  }
                },
                {"$group": {
                    "_id": "$user.id_str",
                    "count": {"$sum": 1},
                    "total_retweet_count":{"$sum":'$retweet_count'},
                    "total_replay_count":{"$sum":'$reply_count'},
                    "total_like_count":{"$sum":'$favorite_count'},
                    "avg_retweet_count":{"$avg":'$retweet_count'},
                    "avg_replay_count":{"$avg":'$reply_count'},
                    "avg_like_count": {"$avg": '$favorite_count'},
                    "max_retweet_count":{"$max":'$retweet_count'},
                    "max_replay_count":{"$max":'$reply_count'},
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
                '年度总发帖量':user_tweet_summuary['count'],
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
    df2.to_excel('./export_data/%s/user_summary/%s.xlsx' % ("twitter","twitter_user_summary_2017"),
                 sheet_name='Sheet1')

#facebook用户各项指标计算
def export_indicator_facebook(db='EsUserPost',collection='es_user_post'):
    client = MongoClient()
    db_tweets = client['%s' % db]
    collect_tweets = db_tweets['%s' % collection]
    client2 = MongoClient("mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin")
    db_user = client2['FaceBook']
    collect_user = db_user['facebook']

    with open(os.path.abspath('./facebook_user_ids.json'), 'r') as f:
        user_ids = json.load(f)
    formatDocs=[]
    for id in user_ids['ids']:
        # 查找该永和的用户信息
        user_for_id = collect_user.find_one({"id": id})
        # 查找该用户下的所有文章
        user_for_id_tweets_count = collect_tweets.count({"user.id":id,'site':'fb'})
        # print(user_for_id_tweets_count)
        if (user_for_id_tweets_count > 0):
            aggregate_for_user_tweets = collect_tweets.aggregate([
                {
                    "$match": {
                        "user.id": id,
                        'site':'fb'
                    }
                },
                {"$group": {
                    "_id": "$user.id",
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
                '年度总发帖量': user_tweet_summuary['count'],
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
    df2.to_excel('./export_data/%s/user_summary/%s.xlsx' % ("facebook", 'facebook_user_summary_2017'),
                 sheet_name='Sheet1')


#导出facebook和twitter情感分析
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
            print(analyzer[0])
            # final_result  = list(concat(list(flatten(list(map(lambda x:x['document_tone']['tones'],analyzer))))))
            # group_result = group_by(lambda x:x['tone_name'],final_result)
            
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

#导出facebook twitter的user用户
def exportFacebookUsers():
    client = MongoClient('mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin')
    dbs = client['%s' % 'FaceBook']
    collections = dbs['%s' % 'facebook']
    with open('facebook_user_ids.json', 'r') as f:
        data = f.read()
        ids = json.loads(data)['ids']
    formatDocs=[]
    for id in ids:
        doc = collections.find_one({'id':id},
                             {
                                 'about':1,
                                 'birthday':1,
                                 'bySheet':1,
                                 'category':1,
                                 'current_location':1,
                                 'description':1,
                                 'emails':1,
                                 'fan_count':1,
                                 'hometown':1,
                                 'keywords':1,
                                 'likes':1,
                                 'link':1,
                                 'location':1,
                                 'name':1,
                                 'website':1,
                                 'likes_num':1


        })
        doc['fan_count'] = int(doc['fan_count'].replace(',', '')) if not doc['fan_count'].endswith(
                    '万') else int(re.sub("\D",'',doc['fan_count'].replace('万', '0000')))
        if 'likes_num' in doc:
            if type(doc['likes_num']) == str:
                doc['likes_count'] = int(doc['likes_num'].replace(',', '')) if not doc['likes_num'].endswith(
                    '万') else int(re.sub("\D",'',doc['likes_num'].replace('万', '0000')))
            else:
                doc['likes_count'] = doc['likes_num']
            del doc['likes_num']
        doc['_id']=str(doc['_id'])
        print(doc)
        formatDocs.append(doc)
    df2 = pd.DataFrame(formatDocs)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                       decode('utf-8') if isinstance(x, str) else x)
    # print(docs)
    df2.to_excel('./export_data/%s/user_summary/%s.xlsx' % ("facebook", "user"),
                     sheet_name='Sheet1')

def exportTwitterUsers():
    client = MongoClient()
    dbs = client['%s' % 'Twitter']
    collections = dbs['%s' % 'twitter']
    with open('twitter_user_ids.json', 'r') as f:
        data = f.read()
        ids = json.loads(data)['ids']
    formatDocs=[]
    for id in ids:
        doc = collections.find_one({'id_str':id},
                             {
                                 'bySheet':1,
                                 'created_at':1,
                                 'description':1,
                                 'favourites_count':1,
                                 'followers_count':1,
                                 'friends_count':1,
                                 'keywords':1,
                                 'listed_count':1,
                                 'location':1,
                                 'name':1,
                                 'screen_name':1,
                                 'statuses_count':1,
                                 'list_num':1,
                                 'moment_num':1,
                                 'isExist':1,
                                 'isVerified':1


        })
        print(doc)
        doc['url']="https://twitter.com/%s" % (doc['screen_name'],)
        doc['_id']=str(doc['_id'])
        formatDocs.append(doc)
    df2 = pd.DataFrame(formatDocs)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                       decode('utf-8') if isinstance(x, str) else x)
    # print(docs)
    df2.to_excel('./export_data/%s/user_summary/%s.xlsx' % ("twitter", "user"),
                     sheet_name='Sheet1')

def testqufeng():
    client = MongoClient('mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin')
    dbs = client['%s' % 'TUserPost']
    collections = dbs['%s' % 'Tuser_post']
    pipline = [
        {"$match": {
            "site": "facebook"
        }},
        {"$group": {
            "_id": "$permalink_url",
            "count": {"$sum": 1}
        }}
    ]
    print(collections.count({'permalink_url':{"$exists":True}}))
    result = list(collections.aggregate(pipline))
    print(len(result))
    countgt1 = []
    counteq1 = []
    for item in result:
        if(item['count'])>1:
            countgt1.append(item)
        else:
            counteq1.append(item)
    print('count>1的个数:%s' % len(countgt1))
    print('count=1的个数:%s' % len(counteq1))

# exportTwitterUsers()

def getArticlesFromKeywords(keywords=[],category='tw'):
    client = MongoClient()
    db_tweets = client['KwUserPost']
    collect_tweets = db_tweets['kw_user_post']
    # db_user = client['FaceBook']
    # collect_user = db_user['facebook']
    for i in range(1,13):
        for kw in keywords:
            if i == 12:
                url = 'http://narnia.idatage.com/stq/api/v1/rowlet/findEsTextByUserIdOrKeywords?startDate=2017-12-01&endDate=2018-01-01&category=%s&keywords=%s' % (category,kw)
            elif i==9:
                url = 'http://narnia.idatage.com/stq/api/v1/rowlet/findEsTextByUserIdOrKeywords?startDate=2017-09-01&endDate=2017-10-01&category=%s&keywords=%s' %(category,kw)
            else:
                url = 'http://narnia.idatage.com/stq/api/v1/rowlet/findEsTextByUserIdOrKeywords?startDate=2017-0%s-01&endDate=2017-0%s-01&category=%s&keywords=%s' % (i,i+1,category,kw) if i in [1,2,3,4,5,6,7,8] else 'http://narnia.idatage.com/stq/api/v1/rowlet/findEsTextByUserIdOrKeywords?startDate=2017-%s-01&endDate=2017-%s-01&category=%s&keywords=%s' % (i,i+1,category,kw)
            articles = asynchronous_request([{
                'url':url
            }])
            if len(articles) == 0:
                continue;
            articles = articles[0]
            # process_articles = (x['_source']=kw for x in articles['tw'])
            def process_data(x, kw=kw):
                x['_source']['keywords'] = kw
                x['_source']['site'] = category
                return x['_source'];

            process_articles = list(map(process_data, articles['tw'] if category=='tw' else articles['fb'] ))
            if len(process_articles) > 0:
            # try:
                # print(next(process_articles))
                for tw in list(process_articles):
                    print(tw)
                    create_at = datetime.strptime(tw['create_at'],'%Y-%m-%dT%H:%M:%S.000Z').timestamp()
                    print(create_at)
                    utctime = datetime.utcfromtimestamp(int(create_at)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    print(utctime)
                    print('\n')
                insert_result = collect_tweets.insert_many(process_articles)
                print(insert_result.inserted_ids)

def import_user_info_to_mongodb():
    client = MongoClient("mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin")
    db_users = client['Twitter']
    users = db_users['twitter']
    items = pd.read_excel('./US_user.xlsx',sheet_name='The Cabinet', index_col = None, na_values = ['NA'])
    df = pd.DataFrame(items)
    keyWordItems = df.values.tolist()[0:]
    for item in keyWordItems:
        # print(item[1],item[0],item[2])
        user_result = users.update_many({'keywords':item[1]},{'$set':{
            'position':item[0],
            'party':item[2],
            'gender':item[3],
            'birth':item[4],
            'age':item[5]
        }})
        print('更新了%s个用户' % user_result.modified_count)

#根据关键词导出文章
def export_keywords_docs(kw,db,col,site):
    client = MongoClient()
    kw_dbs = client['EsUserPost']
    kw_docs = kw_dbs['es_user_post']
    client2 = MongoClient("mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin")
    db_users = client2[db]
    users = db_users[col]
    for k in kw:

        handle_data=[]
        doc_cursor = kw_docs.find({'text':{'$regex':k},'site':site},no_cursor_timeout=True)
        for x in doc_cursor:
            if db=='Twitter':
                user = users.find_one({'id_str': x['user']['id_str']})
                # print(user)
                print(x['id_str'],x['created_at'],x['reply_count'])
                handle_data.append({
                    'ID': str(x['id_str']),
                    '人名': user['name'],
                    '类别': user['position'],
                    '政党': user['party'],
                    'Text': x['text'],
                    '发表时间': x['created_at'],
                    '转发量': x['retweet_count'],
                    '评论量': x['reply_count'],
                    '点赞量': x['favorite_count']
                })
            else:
                user = users.find_one({'id': x['user']['id']})
                print(x)
                handle_data.append({
                    'ID':str(x['_id']),
                    '人名':user['name'],
                    '类别':user['position'],
                    '政党':user['party'],
                    'Text':x['text'],
                    '发表时间':x['create_at'],
                    '转发量':x['share_count'],
                    '评论量':x['comment_num'],
                    '点赞量':x['likes_num']
                })
        doc_cursor.close()
        # print(handle_data)
        #
        df2 = pd.DataFrame(handle_data)
        df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                           decode('utf-8') if isinstance(x, str) else x)
        if db=='Twitter':
            df2.to_excel('./export_data/%s/tweet/%s.xlsx' % ("twitter", "current_%s_articles" % k),
                         sheet_name=k)
        else:
            df2.to_excel('./export_data/%s/post/%s.xlsx' % ("facebook", "current_%s_articles" % k),
                     sheet_name=k)





if __name__=="__main__":
    keywords = ["russia"]
    # keywords = ["India","Indian","Modi"]
    # keywords = ['Tax']
    getArticlesFromKeywords(keywords=keywords,category='fb')
    # exportTwitterUsers()
    # export_keywords_docs(keywords,'Twitter','twitter','tw')

