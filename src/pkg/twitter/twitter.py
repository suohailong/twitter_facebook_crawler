#!/usr/bin/python
#coding:utf-8
# Filename: support.py

import tweepy
import sys
import logging
import os
import json
from collections import Iterable
from datetime import datetime
import sys, time, argparse, json, os, pprint
sys.path.append(".")
from src.pkg.base.base import Base
import twython
import requests
import bs4
import re
import datetime
from pyquery import PyQuery as pq
from src.redis_helper import RedisQueue
import aiohttp

class TWitter(Base,twython.Twython):
    """
    The RequestHandler class for our server.
    """
    def __init__(self,args={}):
        #Base.__init__()
        super(TWitter,self).__init__()

        #以上是俩种调用基类构造函数的方法
        self.__consumer_key = 'c58jPuNxqLex5QttLkoVF621T'
        self.__consumer_secret = "qU2EfulVxZ9a9mSPVm0bww4HXDyC8qk4a2gQrq7bgy4dKOqfup"
        self.__access_token = "930249938012798978-BJCWSdIgciyVZ0IUKLXVXLlc1A3D2my"
        self.__access_secret = "HjDrf1nvRDZIT5NSXioGVeOeZoev26Ibi08hCBQMhMof4"
        super(Base, self).__init__(self.__consumer_key,self.__consumer_secret,self.__access_token,self.__access_secret)

        auth = tweepy.OAuthHandler(self.__consumer_key, self.__consumer_secret)
        auth.set_access_token(self.__access_token, self.__access_secret)
        self.__flag='twitter'
        self.api = tweepy.API(auth)
        self.args = args
        # self.crawler_list_queue = RedisQueue(name='twitter_list',redis_config=redis_config)
        self.crawler_tweets_err_queue = RedisQueue(name='twitter_error', redis_config=self.app_config['redis_config'])
        self.crawler_replay_queue = RedisQueue(name='twitter_replay', redis_config=self.app_config['redis_config'])
        self.crawler_tweets_queue = RedisQueue(name='twitter',redis_config=self.app_config['redis_config'])
        self.twitter_users_queue = RedisQueue(name='twitter_users', redis_config=self.app_config['redis_config'])

    def fetch_user_tweets(self, user_id=None,deadline=None,current_max_id=None, bucket="timelines"):

        if not user_id:
            raise Exception("user_timeline: user_id cannot be None")
        prev_max_id = -1
        if not current_max_id:
            current_max_id = 0
        last_lowest_id = current_max_id  # used to workaround users who has less than 200 tweets, 1 loop is enough...
        cnt = 0
        retry_cnt = 5
        timeline = []  #
        while current_max_id != prev_max_id and retry_cnt > 1:
            try:
                if current_max_id > 0:
                    tweets = self.get_user_timeline(user_id=user_id, max_id=current_max_id - 1, count=20)
                else:
                    tweets = self.get_user_timeline(user_id=user_id, count=20)

                prev_max_id = current_max_id  # if no new tweets are found, the prev_max_id will be the same as current_max_id
                # crawler_replay_list= []
                for tweet in tweets:
                    # print(tweet)
                    if deadline:
                        date = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
                        print(date)
                        deadline_panduan = datetime.datetime.strptime('%s +0000' % deadline,'%Y-%m-%d %z')
                        # print(deadline_panduan)
                        if (date-deadline_panduan).days<=0:
                            break;
                        # list = self.crawler_list_count(tweet['user']['screen_name'])
                        # tweet['list_num']=list
                        tweet['site']='twitter'
                        tweet['latest']='true'
                        tweet['update_status'] = False
                        tweet['update_time'] = datetime.datetime.today()
                        # print('存入mongo')
                        object_id = self.save(tweet)
                        # crawler_replay_list.append("https://twitter.com/%s/status/%s" % (tweet['user']['screen_name'], tweet['id_str']))
                        print('save %s ==>successfuly' % object_id)
                    time_line = re.search(r'\w{3}\sOct\s\d{2}\s\d{2}:\d{2}:\d{2}\s\+\d{4}\s2017',tweet['created_at'])
                    if current_max_id == 0 or current_max_id > int(tweet['id']):
                        current_max_id = int(tweet['id'])
                # if len(crawler_replay_list)>0:
                    # print(crawler_replay_list)
                    # self.crawler_replay_queue.put(crawler_replay_list)
                    # print("推入成功%s个" % len(crawler_replay_list))

                time.sleep(1)
                # no new tweets found
                if (prev_max_id == current_max_id):
                    print('此用户文章抓取完成 %s ' % user_id)
                    break;

            except Exception as e:
                # print('<%s重新加载到文章队列>' % user_id)
                # self.crawler_tweets_err_queue.lput({"user_id":user_id,"current_max_id":current_max_id})
                # posts = self.get_mongod_client()
                # deleteObj = posts.delete_many({'id_str': user_id})
                # print('<清除%s用户的所有文章,文章数为:%s>' % (user_id, deleteObj.deleted_count))
                # break;
                # print(e)
                raise e

    def crawler_list_count(self,user_sreen_name=None,user_id=None):
        try:
            reponse=self.asynchronous_request(
                "https://twitter.com/%s" % user_sreen_name)
            _ = pq(reponse[0]['content'])

            tweet_count = _('ul.ProfileNav-list>li.ProfileNav-item--tweets span.ProfileNav-value').attr('data-count')
            flowing_count = _('ul.ProfileNav-list>li.ProfileNav-item--following span.ProfileNav-value').attr('data-count')
            followers_count = _('ul.ProfileNav-list>li.ProfileNav-item--followers span.ProfileNav-value').attr('data-count')
            favorites_count = _('ul.ProfileNav-list>li.ProfileNav-item--favorites span.ProfileNav-value').attr('data-count')
            list_count = _('ul.ProfileNav-list>li.ProfileNav-item--lists span.ProfileNav-value').text()
            moment_count = _('ul.ProfileNav-list>li.ProfileNav-item--moments span.ProfileNav-value').text()

            # print((tweet_count,flowing_count,followers_count,favorites_count,list_count,moment_count))

            list_count =list_count if list_count else 0
            moment_count = moment_count if moment_count else 0
            flowing_count = flowing_count if flowing_count else 0
            tweet_count = tweet_count if tweet_count else 0
            favorites_count = favorites_count if favorites_count else 0
            followers_count = followers_count if followers_count else 0
            # print(list_count)
            if (tweet_count,followers_count,flowing_count,favorites_count,list_count,moment_count) == (0,0,0,0,0,0):
                if _('.errorpage-body-content>h1').text():
                    print('此页面错误，无法抓取')
                    return (0, 0, 0, 0, 0, 0)
                print('重新加入队列')
                self.twitter_users_queue.lput(user_id)
            return (tweet_count,flowing_count,followers_count,favorites_count,list_count,moment_count)
        except aiohttp.ClientError as e:
            print('重新加入队列')
            self.twitter_users_queue.lput(user_id)
            return (None,None,None,None,None,None)
            # raise e
            # print(e)
            # return None,None
    def crawler_replay_num(self,urls):
        try:
            response = self.asynchronous_request(urls)
            result_list = []
            if response:
                for item in response:
                    # print(item)
                    try:
                        _ = pq(item['content'])
                        replay = _('div.js-tweet-details-fixer.tweet-details-fixer+div.stream-item-footer div.ProfileTweet-actionCountList.u-hiddenVisually span.ProfileTweet-action--reply.u-hiddenVisually>span').attr('data-tweet-stat-count')
                        retweet = _('div.js-tweet-details-fixer.tweet-details-fixer+div.stream-item-footer div.ProfileTweet-actionCountList.u-hiddenVisually span.ProfileTweet-action--retweet.u-hiddenVisually>span').attr('data-tweet-stat-count')
                        like = _('div.js-tweet-details-fixer.tweet-details-fixer+div.stream-item-footer div.ProfileTweet-actionCountList.u-hiddenVisually span.ProfileTweet-action--favorite.u-hiddenVisually>span').attr('data-tweet-stat-count')
                        content = _('p.TweetTextSize.TweetTextSize--jumbo.js-tweet-text.tweet-text').text().replace(r'%s' % _('a.twitter-timeline-link.u-hidden').text(),'')
                        result_list.append({
                            "url":item['url'],
                            "reply_count":replay if replay else 0,
                            "retweet_count":retweet if retweet else 0,
                            "favorite_count":like if like else 0,
                            'content':content
                        })

                    except Exception as e:
                        print(e)
                        result_list.append({
                            "url": item['url'],
                            "reply_count": None,
                            "retweet_count":None,
                            "favorite_count":None,
                            'content':None
                        })
            return result_list
        except Exception as e:
            raise e
        # tweet['reply_count'] = reply_count
        # print(tweet['created_at'])
    def search_users(self, keyword=[],typeIndex=1):
        try:
            def handle(y):
                y = y._json
                if int(typeIndex) == 2:
                    y['searchBy'] = 'EnglishName'
                else:
                    y['searchBy'] = 'ChineseName'
                y['bySheet'] = self.args.sheet
                y['keywords'] = keyword[int(typeIndex)]
                # if(len(keyword)>1):
                #     y['chinaName'] = keyword[1]
                #     y['englishName'] = keyword[2]
                return y

            userList = self.api.search_users(keyword[int(typeIndex)])
            users = list(map(handle, userList))
            if users:
                for somebody in users:
                    print(somebody)
                    id = super().save(somebody)
                    if (id):
                        print('save %s==> ok' % id)
            else:
                print('no data provid')
                # super().saveAsExcel(users,self.__flag,keyword)
        except Exception as e:
            logging.exception(e)
    def get_user_info(self,screen_name=None):
        user_info = self.show_user(screen_name=screen_name)
        id = self.save_user(doc=user_info,dbName='Twitter',collectionName='twitter')
        print('[===%s存储成功===]' % id)


if __name__ == '__main__':
    twitter = TWitter()
    # twitter
    doc = twitter.fetch_user_tweets(user_id='935148134505828353',deadline='2016-12-31')
    # print(twitter.crawler_list_count(user_sreen_name='RepDianaDeGette'))
    # repaly = twitter.crawler_replay_num('https://twitter.com/RepBarragan/status/961656767414439943')

    # with open('twitter_user_ids3.json','r') as f:
    #     user_screen_names = json.load(f)
    # for screen_name in user_screen_names['ids']:
    #     print('[<===screen_name=%s===>]' % screen_name)



