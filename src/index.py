#!/usr/bin/python
#coding:utf-8

import argparse
import time
import sys, time, argparse, json, os, pprint
sys.path.append(".")
from src.shedule import Shedule
from src.pkg.twitter.twitter import TWitter
from src.pkg.facebook.facebook_api import FaceBook
from src.pkg.base.base import Base
import pandas as pd
from src.redis_helper import RedisQueue

def crawler_init(name='twitter'):
    print('<-----初始化程序----->')
    if name == 'twitter':
        twitter_crawler_queue = RedisQueue(name='twitter')
        if twitter_crawler_queue.qsize() == 0:
            with open(os.path.abspath('twitter_user_ids.json'), 'r') as f:
                user_ids = json.load(f)
                for id in user_ids['ids']:
                    twitter_crawler_queue.put(id)
    else:
        facebook_crawler_queue = RedisQueue(name='facebook')
        if facebook_crawler_queue.qsize()==0:
            with open(os.path.abspath('facebook_user_ids.json'), 'r') as f:
                user_ids = json.load(f)
                for id in user_ids['ids']:
                    facebook_crawler_queue.put(id)
    print('<-----初始化成功------>')



def serve():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-c',"--cmd",
                        help="crawler",
                        default="tweets",
                        action="store")
    parser.add_argument('-i', "--init",
                        help="init tweets",
                        default=None,
                        action="store")
    parser.add_argument('-j', '--json',
                        help="the location of the json file that has a list of user_ids or screen_names",
                        required=False)
    parser.add_argument('-s',"--site",
                        help="runner's host, by default it uses localhost",
                        default="twitter",
                        action="store")
    parser.add_argument("--keywords",
                        help="runner's port, by default it uses values range_star",
                        default='STATE GRID',
                        action="store")
    parser.add_argument("--type",
                        help="runner's port, by default it uses values range_star",
                        default=1,
                        action="store")
    parser.add_argument("--sheet",
                        help="runner's port, by default it uses values range_star",
                        default='Senators',
                        #default='中国企业500强',
                        action="store")

    args = parser.parse_args()
    shedule = Shedule()
    if args.cmd=='crawler_tweets':
        if args.site=='twitter':
            if args.init == 'tweets':
                crawler_init(name='twitter')
            shedule.crawler_tweets(TWitter(args),args.site)
        else:
            if args.init == 'tweets':
                crawler_init(name='facebook')
            shedule.crawler_tweets(FaceBook(args),args.site)
    elif args.cmd =='crawler_users':
        items = pd.read_excel('./keywords.xlsx', args.sheet, index_col=None, na_values=['NA'])
        df = pd.DataFrame(items)
        keyWordItems = df.values.tolist()[0:]
        if args.site=='twitter':
            shedule.crawler_users(TWitter(args),keyWordItems,args.type)
        else:
            shedule.crawler_users(FaceBook(args),keyWordItems,args.type)
    elif args.cmd=='Update_twitter_users':
        if args.site=='facebook':
            print('this cmd is only for twitter')
            quit()
        shedule.update_twitter_users_count(TWitter(args))
    elif args.cmd=='crawler_reactions':
        if args.site=='twitter':
            print('this cmd is only for facebook')
            quit()
        shedule.crawler_reactions(FaceBook(args))
    elif args.cmd=='Update_facebook_users':
        if args.site=='twitter':
            print('this cmd is only for facebook')
            quit()
        shedule.update_facebook_users_count(FaceBook(args))
    elif args.cmd=='crawler_tweet_replay':
        if args.site=='facebook':
            print('this cmd is only for twitter')
            quit()
        shedule.crawler_tweets_replay_count(TWitter(args))
    else :
        print('请输入正确的命令')



    # if args.n == 'twitter':
    #    startSys()
    # elif args.n == 'facebook':
    #    startSys()
    # else:
    #     print('没有此项选择')
    




if __name__ == "__main__":
    serve()
