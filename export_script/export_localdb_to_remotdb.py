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
import re
from pyquery import PyQuery as pq
from datetime import datetime
from aiohttp import ClientSession
import aiohttp,os,sys
import asyncio

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

def create_mongo_conn(mnStr='mongodb://127.0.0.1:27017',db="FaceBook",collection="facebook"):
    print('链接==》%s' % mnStr)
    client = MongoClient(mnStr)
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    return collections


def export_localDB_to_remoteDB():
    cli_1 = create_mongo_conn()
    cli_2 = create_mongo_conn(mnStr="mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin")

    result = True;
    i = 0;
    while result:
        result = list(cli_1.find({}).skip(i).limit(300))
        # print(result)
        try:
            o = cli_2.insert_many(result)
            print(o.inserted_ids)
        except Exception as e:
            print(e)

        i+=300

def export_esDate_to_Mongo(ids=[],category='tw'):
    client = MongoClient()
    db_tweets = client['EsUserPost']
    collect_tweets = db_tweets['es_user_post']
    # db_user = client['FaceBook']
    # collect_user = db_user['facebook']
    # for i in range(1,13):
    for id in ids['ids']:

    #         print(id)
    #         if i == 12:
    #             url = 'http://narnia.idatage.com/stq/api/v1/rowlet/findEsTextByUserIdOrKeywords?startDate=2017-12-01&endDate=2018-01-01&category=%s&ids=%s' % (category,id)
    #         elif i==9:
    #             url = 'http://narnia.idatage.com/stq/api/v1/rowlet/findEsTextByUserIdOrKeywords?startDate=2017-09-01&endDate=2017-10-01&category=%s&ids=%s' %(category,id)
#         else:
        print('/n')
        print('抓取到%s用户' % id)
        url = 'http://narnia.idatage.com/stq/api/v1/rowlet/findEsTextByUserIdOrKeywords?startDate=2017-01-01&endDate=2018-01-01&category=%s&ids=%s' % (category,id) #if i in [1,2,3,4,5,6,7,8] else 'http://narnia.idatage.com/stq/api/v1/rowlet/findEsTextByUserIdOrKeywords?startDate=2017-%s-01&endDate=2017-%s-01&category=%s&ids=%s' % (i,i+1,category,id)
        articles = asynchronous_request([{
            'url':url
        }])
        # if len(articles) == 0:
        #     continue;
        articles = articles[0]
        # process_articles = (x['_source']=kw for x in articles['tw'])
        def process_data(x, kw=id):
            x['_source']['keywords'] = kw
            x['_source']['site'] = category
            return x['_source'];

        process_articles = list(map(process_data, articles['tw'] if category=='tw' else articles['fb'] ))
        if len(process_articles) > 0:
        # try:
            # print(next(process_articles))
            print(process_articles)
            insert_result = collect_tweets.insert_many(process_articles)
            print(insert_result.inserted_ids)

def export_fbesData():
    with open(os.path.abspath('./facebook_user_ids.json'), 'r') as f:
        user_ids = json.load(f)
    export_esDate_to_Mongo(user_ids,category='fb')
def export_twesData():
    with open(os.path.abspath('./twitter_user_ids.json'), 'r') as f:
        user_ids = json.load(f)
    # print(user_ids)
    export_esDate_to_Mongo(user_ids,category='tw')

if __name__ == "__main__":
    export_twesData()