from celery import Celery
from celery.utils.log import get_task_logger
import time

import sys, time, argparse, json, os, pprint
from pymongo import MongoClient,DESCENDING,ReturnDocument
sys.path.append(".")
from src.shedule import crawler_tweets_replay

brokers = 'redis://:Abc123098@101.201.227.186:6007/6'
backend = 'redis://:Abc123098@101.201.227.186:6007/7'


app = Celery('tasks', broker=brokers, backend=backend)


def create_mongo_conn(mnStr='127.0.0.1',db="UserPost",collection="user_post"):
    print('connected==>%s' % mnStr)
    client = MongoClient(mnStr)
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    return collections


logger = get_task_logger(__name__)

@app.task(bind=True)
def update_mongo(*items):
    # print('我是参数')
    # print(type(item[1]))
    item = items[1]
    db = create_mongo_conn(db='UserPost', collection='user_post')
    # print(item['url'].split('/')[-1])
    update_doc = db.update_many({"id_str": item['url'].split('/')[-1]}, {
        '$set': {'replay_count': item['reply_count'], 'retweet_count': item['retweet_count'],
                 'favorite_count': item['favorite_count'], "update_status": True}
    })
    print('更新了%s个 replay_count:%s,retweet_count:%s,favorite_count:%s' % (update_doc.modified_count, item['reply_count'], item['retweet_count'], item['favorite_count']))
    # return update_doc.modified_count
    # print(item['url'].split('/')[-1])

@app.task(bind=True)
def crawler_tweet_replay(*tweets):
    return crawler_tweets_replay(*tweets)



# @app.task(bind=True)
# def test_mes(self):
#     for i in range(1, 11):
#         time.sleep(0.1)
#         self.update_state(state="PROGRESS", meta={'p': i*10})
#     return 'finish'