
from pymongo import MongoClient,DESCENDING,ReturnDocument
import sys

from celery import Celery
from celery.utils.log import get_task_logger
import time

import sys, time, argparse, json, os, pprint
from pymongo import MongoClient,DESCENDING,ReturnDocument
sys.path.append(".")
from src.shedule import Shedule
# def create_mongo_conn(mnStr='127.0.0.1',db="UserPost",collection="user_post"):
#     print('connected==>%s' % mnStr)
#     client = MongoClient(mnStr)
#     dbs = client['%s' % db]
#     collections = dbs['%s' % collection]
#     return collections
#
#
# db = create_mongo_conn(db='UserPost', collection='user_post')
# while True:
#     print(db.count({"site":'twitter',"update_status":False
#         # "$or":[
#         #     {"replay_count":None,'site':"twitter"},
#         #     {"retweet_count": None, 'site': "twitter"},
#         #     {"favorite_count": None, 'site': "twitter"},
#         # ]
#     }))
#     tweets = list(db.find({"site":'twitter',"update_status":False
#         # "$or":[
#         #     {"replay_count":None,'site':"twitter"},
#         #     {"retweet_count": None, 'site': "twitter"},
#         #     {"favorite_count": None, 'site': "twitter"},
#         # ]
#     },{"_id":0}).limit(10))
#     if (len(tweets) == 0):
#         break;
#     crawler_tweet_replay.delay(tweets)

if __name__ == '__main__':
    shedule = Shedule()
    shedule.crawler_posts_reactions()