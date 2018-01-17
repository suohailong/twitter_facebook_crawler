from pymongo import MongoClient
import pandas as pd
import numpy as np
import openpyxl
import asyncio,aiohttp,bs4
from aiohttp import ClientSession
import os,sys,json




class Base(object):
    def __init__(self,monStr="mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin"):
        self.saveList = []
        with open(os.path.abspath('config.json'), 'r') as f:
            self.app_config = json.load(f)
            mnStr = self.app_config['mongo_config']['MongoHost']
            self.__dbName = self.app_config['mongo_config']['db']
            self.__collectionName = self.app_config['mongo_config']['collection']
            self.__client = MongoClient(mnStr)
    def asynchronous_request(self, urls):
        if type(urls) == str:
            urls = [urls]
        # print(urls)
        try:
            async def request(url):

                if('url' in url):
                    print('request ===>: %s ' % url['url'])
                    async with ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
                        # session.keep_alive=False
                        try:
                            async with session.get(url['url'], proxy="http://127.0.0.1:51545",
                                                   headers={'CONNECTION': 'close'}) as response:
                                response = await response.read()
                                # print(response.headers)
                                return {
                                    "url": url,
                                    "content": response
                                }
                        except Exception as e:
                            # print('发生了错误')
                            raise Exception('Network_Error')
                else:
                    print('request ===>: %s ' % url)
                    async with ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
                        # session.keep_alive=False
                        try:
                            async with session.get(url, proxy="http://127.0.0.1:51545",
                                                   headers={'CONNECTION': 'close'}) as response:
                                response = await response.read()
                                # print(response.headers)
                                return {
                                    "url": url,
                                    "content": response
                                }
                        except Exception as e:
                            # print('发生了错误')
                            raise Exception('Network_Error')

            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
            tasks = [
                asyncio.ensure_future(request(url)) for url in urls
            ]
            loop.run_until_complete(asyncio.wait(tasks))
            return [task.result() for task in tasks]
        # loop.close()
        except Exception as e:
            if(len(urls)==0):
                print('crawler url is empty')

    def save(self,doc):
        db = self.__client['%s' % self.__dbName]
        collection = db['%s' % self.__collectionName]
        # print(collection)
        result = collection.insert_one(doc)
        return result.inserted_id

    def saveAsExcel(self,docs=[],dir='twitter',fileName='foo'):
        print(docs)
        if docs:
            df2 = pd.DataFrame(docs)
        else:
            print('开始存储excel')
            df2 = pd.DataFrame(self.saveList)
        df2.to_excel('./export/%s/%s.xlsx' % (dir,fileName), sheet_name='Sheet1')
    def run(self,keywords=[]):
        pass
    def saveBefore(self,doc):
        self.saveList.append(doc);