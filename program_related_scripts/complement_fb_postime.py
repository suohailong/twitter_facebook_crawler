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
sys.path.append(".")
from src.redis_helper import RedisQueue
from datetime import datetime



def mongo_login(mnstr='127.0.0.1:27017',db='FaceBook',collection='facebook'):
    client = MongoClient(mnstr)
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    return collections

def timestamp_to_strtime(timestamp):
    local_str_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    return local_str_time


def asynchronous_request(ops=[]):
    async def request(inops=''):
        print('request ===>: %s ' % inops['url'])
        async with ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
            # session.keep_alive=False
            try:
                async with session.get(inops['url'],#proxy="http://127.0.0.1:51545",
                                       headers={'CONNECTION': 'close','CONTENT-TYPE': 'application/json','ACCEPT-LANGUAGE': 'zh-CN,zh;q=0.9,en;q=0.8'}) as response:
                    result = await response.text()
                    if re.search(r'data-utime="\d+"',result):
                        create_at = re.search(r'data-utime="\d+"',result).group()
                        inops['create_at'] = timestamp_to_strtime(int(re.search(r'\d+',create_at).group()))
                        inops["update_status"] = True
                        print('北京时间')
                        print(datetime.fromtimestamp(int(re.search(r'\d+',create_at).group())).strftime('%Y-%m-%dT%H:%M:%S.000Z'))
                    del inops['url']
                    headers = {'content-type': 'application/json'}
                    cun_result= requests.post(url='http://59.110.52.213/stq/api/v1/pa/topicRowletFacebook/add',data=json.dumps([inops]),headers=headers)
                    print('%s推送es成功  ===create_at:%s===' % (inops['id'],inops['create_at']))
                    print(cun_result.text)
                    return cun_result.text
            except Exception as e:
               print('访问出了错误')
               print(inops['permalink_url'])
               print(e)
               return json.loads({"success" : "false"})

    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    tasks = [
        asyncio.ensure_future(request(inops=item)) for item in ops
    ]
    # tasks = [hello('https://www.facebook.com/'), hello('https://www.facebook.com/')]
    loop.run_until_complete(asyncio.wait(tasks))
    return [json.loads(task.result()) for task in tasks]

def completion_fb_posTime(conn=None):
    # with open('twitter_user_ids.json','r') as f:
    #     ids = json.load(f)
    current = 0;
    # for id in ids['ids']:
    with open(os.path.abspath('config.json'), 'r') as f:
        app_config = json.load(f)
    twitter_crawler_queue = RedisQueue(name='fb_test_ids', redis_config=app_config['redis_config2'])
    while True:
        try:
            # print(id)
            if twitter_crawler_queue.empty():
                print('所有的用户全部跑完')
                break
            id = twitter_crawler_queue.get()
            print('[===取出的ID为%s,目前还有%s条需要抓取====]' % (id,twitter_crawler_queue.qsize()))
            es_url = 'http://narnia.idatage.com/stq/api/v1/rowlet/findEsTextByUserIdOrKeywords?startDate=2016-12-30&endDate=2018-02-12&category=fb&ids=%s' % (id,)  # if i in [1,2,3,4,5,6,7,8] else 'http://narnia.idatage.com/stq/api/v1/rowlet/findEsTextByUserIdOrKeywords?startDate=2017-%s-01&endDate=2017-%s-01&category=%s&ids=%s' % (i,i+1,category,id)
            es_body = requests.get(es_url)
            # print('取出的内容为：')
            # print(es_body.text)
            es_body_tw = json.loads(es_body.text)['fb']
            print(len(es_body_tw))
            def handele(x):
                # print(x)
                x['_source']['index_name'] = x['_index']
                x['_source']['type_name'] = x['_type']
                x['_source']['id']=x['_id']
                x['_source']['url'] = x['_source']['permalink_url']
                return x['_source'];
            es_body_tw_urls = list(map(handele,filter(lambda x:"update_status" not in x['_source'],es_body_tw)))
            # print(es_body_tw_urls)
            if len(es_body_tw_urls)>200:
                pool = mp.Pool()
                res = pool.map(asynchronous_request,(es_body_tw_urls[i:i+200] for i in range(0,len(es_body_tw_urls),200)))
                # current += 1;
                print('更新%s用户' % id)
            elif 0<len(es_body_tw_urls)<200:
                asynchronous_request(ops=es_body_tw_urls)
                # current += 1;
                print('更新%s用户' % id)
                # print('第几%s个' % current)
            else:
                current += 1;
                print('该用户%s无需更新' % id)
                print('第几%s个' % current)
            # conn.send(id)
        except Exception as e:
            current = 0
            # print(e)
            raise e
            # continue









if __name__== '__main__':
    # timestr = timestamp_to_strtime(1517583334)
    # print(timestr)
    # completion_fb_posTime()
    parent_conn, child_conn = mp.Pipe()

    p1 = mp.Process(target=completion_fb_posTime, args=(child_conn,))
    p1.start()
    while True:
        if p1.is_alive():
            if parent_conn.poll(200):
                print('接收到消息')
                print('%s用户已更新完毕' % parent_conn.recv())
            else:
                print('\n')
                print('程序卡住，重新启动程序')
                print('\n')
                p1.terminate()
                p1 = mp.Process(target=completion_fb_posTime, args=(child_conn,))
                p1.start()
                # print('我跑我的你跑你的')
        else:
            print('程序跑完，结束')
            break;