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

    def fetch_user_tweets(self, user_id=None,deadline=None, bucket="timelines"):

        if not user_id:
            raise Exception("user_timeline: user_id cannot be None")
        prev_max_id = -1
        current_max_id = 0
        last_lowest_id = current_max_id  # used to workaround users who has less than 200 tweets, 1 loop is enough...
        cnt = 0
        retry_cnt = 5
        timeline = []  #
        while current_max_id != prev_max_id and retry_cnt > 1:
            try:
                if current_max_id > 0:
                    tweets = self.get_user_timeline(user_id=user_id, max_id=current_max_id - 1, count=200)
                else:
                    tweets = self.get_user_timeline(user_id=user_id, count=200)

                prev_max_id = current_max_id  # if no new tweets are found, the prev_max_id will be the same as current_max_id

                for tweet in tweets:
                    response = self.asynchronous_request("https://twitter.com/%s/status/%s" % (tweet['user']['screen_name'], tweet['id']))
                    # s = requests.Session()
                    # response = requests.get(
                    #     "https://twitter.com/%s/status/%s" % (tweet['user']['screen_name'], tweet['id']))
                    # # print(response.text)
                    bs = bs4.BeautifulSoup(response[0]['content'], 'html.parser')
                    html = bs.select(
                        'button.ProfileTweet-actionButton.js-actionButton.js-actionReply span.ProfileTweet-actionCountForPresentation')[
                        0]
                    reply_count = html.text
                    tweet['reply_count'] = reply_count
                    tweet['site'] = 'twitter'
                    # print(tweet['created_at'])
                    if deadline:
                        date = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
                        deadline_panduan = datetime.datetime.strptime('%s +0000' % deadline,'%Y-%m-%d %z')
                        if (date-deadline_panduan).days<=0:
                            break;
                        list = self.crawler_list_count(tweet['user']['screen_name'])
                        tweet['list_num']=list
                        tweet['site']='twitter'
                        tweet['latest']='true'
                        # print('存入mongo')
                        object_id = self.save(tweet)
                        print('save %s ==>successfuly' % object_id)
                    # time_line = re.search(r'\w{3}\sOct\s\d{2}\s\d{2}:\d{2}:\d{2}\s\+\d{4}\s2017',tweet['create_at'])
                    if current_max_id == 0 or current_max_id > int(tweet['id']):
                        current_max_id = int(tweet['id'])


                # no new tweets found
                if (prev_max_id == current_max_id):
                    print('此用户文章抓取完成 %s ' % user_id)
                    break;

            except Exception as e:
                print(e)

    def crawler_list_count(self,user_sreen_name=None):
        try:
            reponse=self.asynchronous_request(
                "https://twitter.com/%s" % user_sreen_name)
            # print(response.text)
            bs = bs4.BeautifulSoup(reponse[0]['content'], 'html.parser')
            html = bs.select(
                '#page-container > div.ProfileCanopy.ProfileCanopy--withNav.ProfileCanopy--large.js-variableHeightTopBar > div > div.ProfileCanopy-navBar.u-boxShadow > div > div > div.Grid-cell.u-size2of3.u-lg-size3of4 > div > div > ul > li.ProfileNav-item.ProfileNav-item--lists > a > span.ProfileNav-value')[
                0]
            list_count = html.text
            # print(list_count)
            return list_count
        except Exception as e:
            print(e)
            return 0

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



if __name__ == '__main__':
    twitter = TWitter()
    #doc = twitter.fetch_user_timeline(user_id='2543008999',deadline='2017-11-10')
    # twitter.crawler_list_count(user_sreen_name='mike_pence')


