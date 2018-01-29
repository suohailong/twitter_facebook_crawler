from pymongo import MongoClient,DESCENDING,ReturnDocument
from bson import objectid
import openpyxl
from openpyxl.workbook import Workbook
from openpyxl.writer.excel import ExcelWriter
import openpyxl.cell
from openpyxl.reader.excel import load_workbook
import pandas as pd
import numpy as np
import pprint
import json,time
import re,os,math,sys
from pyquery import PyQuery as pq
from datetime import datetime
#mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin
sys.path.append(".")
from src.pkg.twitter.twitter import TWitter
from src.pkg.facebook.facebook_api import FaceBook
from src.redis_helper import RedisQueue
from es_srcipt.idatage_export import Espusher

class Shedule(object):
    def __init__(self):
        with open(os.path.abspath('config.json'), 'r') as f:
            self.app_config = json.load(f)
            # print(self.app_config)


    def create_mongo_conn(self,mnStr=None,db=None,collection=None):
        if not mnStr:
            mnStr = self.app_config['mongo_config']['MongoHost']
        if not db:
            db = self.app_config['mongo_config']['db'],
        if not collection:
            collection = self.app_config['mongo_config']['collection']
        print('connected==>%s' % mnStr)
        client = MongoClient(mnStr)
        dbs = client['%s' % db]
        collections = dbs['%s' % collection]
        # print(collections)
        return collections

    def update_twitter_users_count(self,crawler):#twitter 用户的list字段
        # with open(os.path.abspath('twitter_user_ids.json'), 'r') as f:
        #     user_ids = json.load(f)
        # count=1;
        db = self.create_mongo_conn(db='Twitter', collection='twitter')
        while True:
            try:
                tweets = list(db.find({"list_num": None, "update_list":{'$exists':False}}).limit(50))
                print(db.count({"list_num": None,"update_list":{'$exists':False}}))
                if (len(tweets) == 0):
                    break;
                for item in tweets:
                    # print(count)
                    # db=self.create_mongo_conn(db='Twitter',collection='twitter')
                    # doc = db.find_one({"id_str":id})
                    tweet_count, flowing_count, followers_count, favorites_count, list_count, moment_count = crawler.crawler_list_count(item["screen_name"])
                    print(tweet_count,flowing_count,followers_count,favorites_count,list_count,moment_count)
                    after_doc = db.find_one_and_update({"id_str":item['id_str']},{"$set":{
                        "list_num":list_count,
                        "moment_num":moment_count,
                        # "followers_count":followers_count,
                        # "friends_count":flowing_count,
                        # "statuses_count":tweet_count,
                        # "favourites_count":favorites_count
                        "update_list":True
                    }},return_document=ReturnDocument.AFTER)
                    print('更新%s成功' % after_doc['_id'])
                    # count +=1;
            except Exception as e:
                print(e)
                print(id)
                continue

    def crawler_users(self,crawler, kwords, typeIndex):
        for kw in kwords:
            print('\n')
            print('--------start kw=>%s----------' % kw[int(typeIndex)])
            crawler.search_users(kw, typeIndex)
            print('----------end kw=>%s----------\n' % kw[int(typeIndex)])
            time.sleep(1)
        print('all kws is processed')

    def crawler_tweets(self,crawler,site='facebook',deadtime='2017-1-1'):
        print('<-----启动文章抓取----->')
        weipa_count = 1
        if site=='twitter':
            twitter_crawler_queue = RedisQueue(name='twitter',redis_config=self.app_config['redis_config'])
            twitter_crawler_error_queue = RedisQueue(name='twitter_error',redis_config=self.app_config['redis_config'])
            while True:
                if twitter_crawler_error_queue.qsize()>0:
                    err_item = twitter_crawler_error_queue.get()
                    print('取出出错条目:%s',err_item)
                    current_max_id = err_item['current_max_id']
                    id = err_item['user_id']
                    crawler.fetch_user_tweets(user_id=id,current_max_id=current_max_id,deadline=deadtime)
                else:
                    print('[===未抓取的的id个数为:%s===]' % twitter_crawler_queue.qsize())
                    if twitter_crawler_queue.empty():
                        if weipa_count >= 3:
                            print('<-----文章抓取完成----->')
                            break
                        else:
                            weipa_count += 1
                            print('[==Retry:%s==]' % (weipa_count - 1))
                            time.sleep(5)
                            continue

                    id = twitter_crawler_queue.get()
                    crawler.fetch_user_tweets(user_id=id, deadline=deadtime)
                weipa_count = 1;
        else:
            facebook_crawler_queue = RedisQueue(name='facebook',redis_config=self.app_config['redis_config'])
            facebook_crawler_error_queue = RedisQueue(name='facebook_error', redis_config=self.app_config['redis_config'])
            db = self.create_mongo_conn(db='FaceBook', collection='facebook')
            while True:
                if facebook_crawler_error_queue.qsize()>0:
                    err_item = facebook_crawler_error_queue.get()
                    print('取出出错条目:%s',err_item)
                    id = err_item['id']
                    url = err_item['url']
                    if url:
                        crawler.fetch_user_tweets(id=id, urls=url, deadline=deadtime)
                else:
                    print('[===未抓取的的id个数为:%s===]' % facebook_crawler_queue.qsize())
                    if facebook_crawler_queue.empty():
                        if weipa_count >=3:
                            print('<-----文章抓取完成----->')
                            break
                        else:
                            weipa_count+=1
                            print('[==Retry:%s==]' % (weipa_count-1))
                            time.sleep(5)
                            continue

                    id = facebook_crawler_queue.get()
                    doc = db.find_one({"id":str(id)})
                    crawler.fetch_user_tweets(id=id,urls=doc['link']+'posts/',deadline=deadtime)
            # print('完成全部抓取')
                weipa_count = 1;

    def crawler_reactions(self,crawler,history=False):#facebook posts表中的各种量
        print('<=====启动facebook_reactions抓取====>')
        db = self.create_mongo_conn()
        crawler_reactions_queue = RedisQueue(name='facebook_reactions',redis_config=self.app_config['redis_config'])
        weipa_count = 1;
        err_count = 1;
        if not history:
            es = Espusher()
        while True:
            try:
                # print(db.count({"site": "facebook","update_status":False}))
                # tweets = list(db.find({"site": "facebook","update_status":False}).limit(20))
                # if (len(tweets) == 0):
                #     print('全部爬取完成')
                #     break;
                print('[===未抓取的个数为:%s===]' % crawler_reactions_queue.qsize())
                # if crawler_reactions_queue.empty():
                #     if weipa_count >=3:
                #         print('<-----facebook_reactions抓取完成----->')
                #         break
                #     else:
                #         weipa_count+=1
                #         print('[==Retry:%s==]' % (weipa_count-1))
                #         time.sleep(10)
                #         continue
                urls = crawler_reactions_queue.get()#map(lambda x:{"url":'https://facebook.com%s' % x['permalink_url'],'id':x['_id']},tweets)
                content = crawler.crawler_reactions_nums(urls)
                # print(content)
                if not content:continue
                for item in content:
                    # print(item)
                    print(item['reactions'])
                    # print(item['url'])
                    # print(url)
                    if not item['reactions']:
                        print(item)
                    else:
                        if history:
                            print(objectid.ObjectId(item['url']['id']))
                            update_doc = db.find_one_and_update({"_id": objectid.ObjectId(item['url']['id'])}, {
                                '$set': {'comment_num': item['reactions']['comment_count'],
                                         'likes_num': item['reactions']['likes_count'],
                                         'share_count':item['reactions']["share_count"],
                                         "update_status": True
                                         }
                            },return_document=ReturnDocument.AFTER)
                            if update_doc != None:
                                print('更新了%s个' % update_doc['_id'])
                        else:
                            data = db.find_one_and_delete({'_id':objectid.ObjectId(item['url']['id'])})
                            data['comment_num'] = item['reactions']['comment_count']
                            data['likes_num'] = item['reactions']['likes_count']
                            data['share_count'] = item['reactions']["share_count"]
                            es.facebook_pusher(data)
                weipa_count = 1;
                err_count = 1;
            except Exception as e:
                print(e)
                continue;

    def update_facebook_users_count(self,crawler):#facebook 用户账户的like_num字段
        db = self.create_mongo_conn(db='FaceBook', collection='facebook')
        while True:
            try:
                tweets = list(db.find({"likes_num":0,"update_likes":{'$exists':False}}).limit(20))
                print(db.count({"likes_num":0,"update_likes":{'$exists':False}}))
                if(len(tweets)==0):
                    break;
                def makeUrl(x):
                    s = x['link']
                    if s.endswith('/'):
                        return x['link']+'community/'
                    else:
                        return x['link']+'/'+'community/'
                urls = list(map(makeUrl,tweets))
                content = crawler.crawler_user_likes(urls)

                for item in content:
                    url = item['url'].replace('community/','') if not item['url'].endswith('/community/') else item['url'].replace('/community/','')
                    # print(url)
                    print(item)
                    if "isLoginStatus" in item:
                        update_doc = db.update_many({"link": url}, {
                            '$set': {
                                'likes_num': item['like_count'],
                                # 'fan_count': item['fan_count'],
                                "update_likes": True,
                                "update_status": True,
                                "isLoginStatus":item["isLoginStatus"]
                            }
                        })
                        print('更新了%s个' % update_doc.modified_count)
                    else:
                        update_doc = db.update_many({"link": url}, {
                            '$set': {
                                'likes_num': item['like_count'],
                                # 'fan_count':item['fan_count'],
                                "update_status":True,
                                "update_likes": True
                            }
                        })
                        print('更新了%s个' % update_doc.modified_count)
            except Exception as e:
                raise e
                continue

    #lldlddl
    def crawler_tweets_replay_count(self,crawler,history=False):#推文表中的replay_count字段
        print('<=====启动tweet_replay抓取====>')
        db = self.create_mongo_conn()
        crawler_tweet_replay_queue = RedisQueue(name='twitter_replay',redis_config=self.app_config['redis_config'])
        weipa_count = 1;
        err_count = 1;
        if not history:
            es = Espusher()
        while True:
            try:
                print('[===未抓取的个数为:%s===]' % crawler_tweet_replay_queue.qsize())
                # if crawler_tweet_replay_queue.empty():
                #     if weipa_count >=3:
                #         print('<-----twitter_replay抓取完成----->')
                #         break
                #     else:
                #         weipa_count+=1
                #         print('[==Retry:%s==]' % (weipa_count-1))
                #         time.sleep(10)
                #         continue
                # print(weipa_count)
                urls = crawler_tweet_replay_queue.get()
                print("取出%s个" % len(urls))
                content = crawler.crawler_replay_num(urls)
                for item in content:
                    print(item)
                    # print(item['url'].split('/')[-1])
                    if history:
                        update_doc = db.update_many({"id_str": item['url'].split('/')[-1],'site':'twitter'}, {
                            '$set': {'replay_count': item['reply_count'], 'retweet_count': item['retweet_count'],
                                     'favorite_count': item['favorite_count'], "update_status": True}
                        })
                        print('更新了%s个' % (
                        update_doc.modified_count))
                    else:
                        print('push item to es')
                        # print(item)
                        data = db.find_one_and_delete({"id_str": item['url'].split('/')[-1],'site':'twitter'})
                        data['replay_count'] = item['reply_count']
                        data['favorite_count'] = item['favorite_count']
                        data['retweet_count'] = item['retweet_count']
                        es.twitter_pusher(data)
                weipa_count=1;
                err_count = 1;
            except Exception as e:
              print(e)
              continue

    def crawler_tweets_replay(self,history=False):#推文表中的replay_count字段  用celery做
        print('<=====启动tweet_replay抓取====>')
        crawler = TWitter()
        db = self.create_mongo_conn()
        # crawler_tweet_replay_queue = RedisQueue(name='twitter_replay', redis_config=self.app_config['redis_config'])
        # weipa_count = 1;
        if not history:
            es = Espusher()
        while True:
            try:
                print(db.count({"update_status":False,'site':"twitter"}))
                tweets = list(db.find({"update_status":False,'site':"twitter"}).limit(20))
                if (len(tweets) == 0):
                    break;
                urls = list(map(lambda tweet:"https://twitter.com/%s/status/%s" % (tweet['user']['screen_name'], tweet['id_str']),tweets))
                content = crawler.crawler_replay_num(urls)
                for item in content:
                    if history:
                        update_doc = db.update_many({"id_str": item['url'].split('/')[-1],'site':'twitter'}, {
                            '$set': {'replay_count': item['reply_count'], 'retweet_count': item['retweet_count'],
                                     'favorite_count': item['favorite_count'], "update_status": True}
                        })
                        print('更新了%s个' % (
                        update_doc.modified_count))
                    else:
                        print('push item to es')
                        # print(item)
                        data = db.find_one_and_delete({"id_str": item['url'].split('/')[-1],'site':'twitter'})
                        data['replay_count'] = item['reply_count']
                        data['favorite_count'] = item['favorite_count']
                        data['retweet_count'] = item['retweet_count']
                        es.twitter_pusher(data)

            except Exception as e:
                print(e)
                continue
    def crawler_posts_reactions(self,history=False):
        print('<=====启动facebook_reactions抓取====>')
        crawler = FaceBook()
        db = self.create_mongo_conn()
        if not history:
            es = Espusher()
        while True:
            try:
                print(db.count({"site": "facebook","update_status":False}))
                tweets = list(db.find({"site": "facebook","update_status":False}).limit(50))
                if (len(tweets) == 0):
                    print('全部爬取完成')
                    break;
                urls = map(lambda x:{'url':'https://facebook.com%s' % x['permalink_url'] if not x['permalink_url'].startswith('https') else x['permalink_url'],'id':str(x['_id'])},tweets)
                content = crawler.crawler_reactions_nums(urls)
                # print(content)
                if not content: continue
                for item in content:
                    # print(item)
                    # print(item['reactions'])
                    # print(item['url'])
                    # print(url)
                    if not item['reactions']:
                        print(item)
                    else:
                        if history:
                            print(objectid.ObjectId(item['url']['id']))
                            update_doc = db.find_one_and_update({"_id": objectid.ObjectId(item['url']['id'])}, {
                                '$set': {'comment_num': item['reactions']['comment_count'],
                                         'likes_num': item['reactions']['likes_count'],
                                         'share_count': item['reactions']["share_count"],
                                         "update_status": True
                                         }
                            }, return_document=ReturnDocument.AFTER)
                            if update_doc != None:
                                print('更新了%s个' % update_doc['_id'])
                        else:
                            data = db.find_one_and_delete({'_id': objectid.ObjectId(item['url']['id'])})
                            data['comment_num'] = item['reactions']['comment_count']
                            data['likes_num'] = item['reactions']['likes_count']
                            data['share_count'] = item['reactions']["share_count"]
                            es.facebook_pusher(data)

            except Exception as e:
                # raise e;
                print(e)
                continue