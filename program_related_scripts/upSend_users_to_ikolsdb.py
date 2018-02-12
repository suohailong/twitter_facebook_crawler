import openpyxl,json
from openpyxl.workbook import Workbook
from openpyxl.writer.excel import ExcelWriter
import openpyxl.cell
from openpyxl.reader.excel import load_workbook
import pandas as pd
import numpy as np
from pymongo import MongoClient,DESCENDING
import requests
from pyquery import PyQuery as pq
from aiohttp import ClientSession
import aiohttp,os,sys
import asyncio,re
import multiprocessing as mp



def mongo_login(mnstr='127.0.0.1:27017',db='FaceBook',collection='facebook'):
    client = MongoClient(mnstr)
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    return collections


def asynchronous_request_facebook_api(ops=[]):
    async def request(url='', formdata={},headers={"request-id":"880c46-6d64fb-879b-0de9-b1c1b84180f4",'CONNECTION': 'close','content-type': 'application/json'}):
        print('request ===>: %s ' % url)
        async with ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
            # session.keep_alive=False
            # print(formdata)
            async with session.post(url,
                                    data=json.dumps(formdata),
                                    headers=headers) as response:
                result = await response.text()
                # print(result)
                return result

                # print(response)
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    tasks = [
        asyncio.ensure_future(request(url=item['url'], formdata=item['data'],headers=item['headers'])) for item in ops
    ]
    # tasks = [hello('https://www.facebook.com/'), hello('https://www.facebook.com/')]
    loop.run_until_complete(asyncio.wait(tasks))
    return [json.loads(task.result()) for task in tasks]


def add_new_users_to_ikols(db=None,collection=None,config='facebook_user_ids.json'):
    fb = mongo_login(mnstr="mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin",db=db,collection=collection)
    with open(config,'r') as f:
        urls = json.load(f)

    for id in urls['ids']:

        try:
            if db=='Twitter':
                # print(fb)
                doc = fb.find_one({"id_str": id}, {"_id": 0,'update_time':0,'gender':0,'party':0})
                # print(doc)
                result = asynchronous_request_facebook_api([{
                    'url': 'http://ikols.idatage.com/v1/config/account/add.json',
                    'headers': {"request-id": "880c46-6d64fb-879b-0de9-b1c1b84180f4", 'CONNECTION': 'close',
                                'content-type': 'application/json'},
                    'data': {
                        'tw': [
                            doc
                        ],
                        'fb':[]
                    }
                }])
                print('推送结果为：%s,user:%s' % (result, id))
            else:
                doc = fb.find_one({"id": id}, {"_id": 0, 'update_time': 0,'monitor':0,'gender':0})
                if 'likes_num' in doc:
                    if type(doc['likes_num']) == str:
                        doc['likes_count'] = int(doc['likes_num'].replace(',', '')) if not doc['likes_num'].endswith(
                            '万') else  int(re.sub("\D", '', doc['fan_count'].replace('万', '0000')))
                    else:
                        doc['likes_count'] = doc['likes_num']
                    del doc['likes_num']

                if 'fan_count' in doc:
                    if type(doc['fan_count']) == str:
                        doc['fan_count'] = int(doc['fan_count'].replace(',', '')) if not doc['fan_count'].endswith(
                            '万') else int(re.sub("\D", '', doc['fan_count'].replace('万', '0000')))

                # doc['likes_num']=int(doc['likes_num'])
                doc['fan_count'] = int(doc['fan_count'])


                # print(doc)
                result = asynchronous_request_facebook_api([{
                    'url': 'http://ikols.idatage.com/v1/config/account/add.json',
                    'headers': {"request-id": "880c46-6d64fb-879b-0de9-b1c1b84180f4", 'CONNECTION': 'close',
                               'content-type': 'application/json'},
                    'data': {
                        'tw':[],
                        'fb':[
                            doc
                        ]
                    }
                }])
                # print(doc['about'])
                print('推送结果为：%s,user:%s' % (result,id))
        except Exception as e:
            print(e)
            print('出错id为:%s' % id)

def update_ikols_user_info(db=None,collection=None,config='twitter_user_ids.json'):
    fb = mongo_login(mnstr="mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin", db=db,
                     collection=collection)
    with open(config, 'r') as f:
        urls = json.load(f)

    for id in urls['ids']:

        try:
            if db == 'Twitter':
                # print(fb)
                doc = fb.find_one({"id_str": id}, {"_id": 0, 'update_time': 0, 'gender': 0, 'party': 0})
                # print(doc)
                result = asynchronous_request_facebook_api([{
                    'url': 'http://ikols.idatage.com/v1/config/account/add.json',
                    'headers': {"request-id": "880c46-6d64fb-879b-0de9-b1c1b84180f4", 'CONNECTION': 'close',
                                'content-type': 'application/json'},
                    'data': {
                        'tw': [
                            {
                                "account_id": doc['id_str'],
                                "followers_count": doc['followers_count'],
                                "favouritesCount": doc['favourites_count'],
                                "friendsCount": doc['friends_count'],
                                "verified": 1 if doc['isVerified'] else 0,
                                "status":1 if doc['isExist'] else 0
                            }
                        ],
                        'fb': []
                    }
                }])
                print('推送结果为：%s,user:%s' % (result, id))
            else:
                doc = fb.find_one({"id": id}, {"_id": 0, 'update_time': 0, 'monitor': 0, 'gender': 0})
                if 'likes_num' in doc:
                    if type(doc['likes_num']) == str:
                        doc['likes_count'] = int(doc['likes_num'].replace(',', '')) if not doc['likes_num'].endswith(
                            '万') else  int(re.sub("\D", '', doc['fan_count'].replace('万', '0000')))
                    else:
                        doc['likes_count'] = doc['likes_num']
                    del doc['likes_num']

                if 'fan_count' in doc:
                    if type(doc['fan_count']) == str:
                        doc['fan_count'] = int(doc['fan_count'].replace(',', '')) if not doc['fan_count'].endswith(
                            '万') else int(re.sub("\D", '', doc['fan_count'].replace('万', '0000')))

                # doc['likes_num']=int(doc['likes_num'])
                doc['fan_count'] = int(doc['fan_count'])

                # print(doc)
                result = asynchronous_request_facebook_api([{
                    'url': 'http://ikols.idatage.com/v1/config/account/add.json',
                    'headers':{"request-id":"880c46-6d64fb-879b-0de9-b1c1b84180f4",'CONNECTION': 'close','content-type': 'application/json'},
                    'data': {
                        'tw': [],
                        'fb': [
                            {
                                "account_id": doc['id'],
                                "fan_count": doc['fan_count'],
                                "likesCount": doc['likes_count'],
                                "verificationStatus":  1 if doc['isVerified'] else 0,
                                "status": 1 if doc['isExist'] else 0
                            }
                        ]
                    }
                }])
                # print(doc['about'])
                print('推送结果为：%s,user:%s' % (result, id))
        except Exception as e:
            print(e)
            print('出错id为:%s' % id)

if __name__== '__main__':
    update_ikols_user_info(db='FaceBook',collection='facebook',config='facebook_user_ids.json')