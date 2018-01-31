from pymongo import MongoClient,DESCENDING,ReturnDocument
from bson import objectid
import openpyxl
from openpyxl.workbook import Workbook
from openpyxl.writer.excel import ExcelWriter
import openpyxl.cell
from openpyxl.reader.excel import load_workbook
import pandas as pd
import pprint
from pyquery import PyQuery as pq
from datetime import datetime
import asyncio,os,json,re
from aiohttp import ClientSession
import aiohttp
import hashlib,math
from bson import json_util

pp = pprint.PrettyPrinter(indent=4)


class Espusher(object):
    def __init__(self):
        super(Espusher,self).__init__()
        with open(os.path.abspath('config.json'), 'r') as f:
            config = json.load(f)
        self.client = MongoClient('mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin&maxPoolSize=8')#config['mongo_config']['MongoHost']
        # self.client = MongoClient('mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin')
        self.dbs = self.client.TUserPost #config['mongo_config']['db']
        # self.collections = dbs['%s' % 'user_post']#config['mongo_config']['collection']

    def asynchronous_request_facebook_api(self,ops=[]):
            async def request(url='',formdata={}):
                print('request ===>: %s ' % url)
                async with ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
                    # session.keep_alive=False

                        async with session.post(url,
                                               data=formdata,
                                               headers={'CONNECTION': 'close','content-type': 'application/json'}) as response:
                            result = await response.text()
                            # print(result)
                            return result

                        # print(response)

            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
            tasks = [
                asyncio.ensure_future(request(url=item['url'],formdata=item['data'])) for item in ops
            ]
            # tasks = [hello('https://www.facebook.com/'), hello('https://www.facebook.com/')]
            loop.run_until_complete(asyncio.wait(tasks))
            return [json.loads(task.result()) for task in tasks]

    def makeId(self,mdstr):
        m = hashlib.md5(mdstr.encode(encoding='utf-8'))
        return m.hexdigest()
    def rsetStatus(self):
        doc = self.dbs.user_post.update_many({'site':'facebook','es_pushed':True},{'$set':{'es_pushed':False}})
        print(doc.modified_count)
    #导出twitter用户的所有文章
    def run_twitter_pusher(self,conn=None,project={}):
            while True:
                # print('开始跑程序')
                try:
                    # es_data =
                    # count_list = list(es_data)
                    # if len(count_list) == 0:
                    #     print('twitter所有文章都推完')
                    #     break;
                    tweets_count = self.dbs.Tuser_post.count({'site':'twitter','update_status':True,'es_pushed':{"$exists":False}})
                    print('\n')
                    print('未推送到es的还有%s个\n' % tweets_count)
                    if tweets_count==0:
                        print('所有的推文推送完毕')
                        break;
                    for item in self.dbs.Tuser_post.find({'site':'twitter','update_status':True,'es_pushed':{"$exists":False}}).limit(20):
                    # try:
                        # print(item)
                        if not isinstance(item['created_at'], datetime):
                            item['created_at'] = datetime.strptime(item['created_at'],
                                                                   '%a %b %d %H:%M:%S %z %Y').strftime(
                                '%Y-%m-%dT%H:%M:%S.000Z')
                        else:
                            item['created_at'] = item['created_at'].strftime(
                                '%Y-%m-%dT%H:%M:%S.000Z')
                        item['user']['created_at'] = datetime.strptime(item['user']['created_at'], '%a %b %d %H:%M:%S %z %Y').strftime('%Y-%m-%dT%H:%M:%S.000Z')
                        hashtags = item['entities']['hashtags'] if 'hashtags' in item['entities'] else []
                        item['entities']['hashtags'] = list(map(lambda x:x['text'],hashtags)) if len(hashtags) != 0 else []
                        # print(item['entities'])
                        twitter_es_data = {
                            'index_name':'rowlet_twitter_articles',
                            'type_name':'rowlet_twitter_articles',
                            'id':item['id'],
                            'id_str':item['id_str'],
                            'text':item['text'],
                            'source':item['source'],
                            'created_at':item['created_at'],
                            'truncated':item['truncated'],
                            'in_reply_to_status_id':item['in_reply_to_status_id'],
                            'in_reply_to_status_id_str':item['in_reply_to_status_id_str'],
                            'in_reply_to_screen_name':item['in_reply_to_screen_name'],
                            'in_reply_to_user_id':item['in_reply_to_user_id'],
                            'in_reply_to_user_id_str':item['in_reply_to_user_id_str'],
                            'user':item['user'],
                            'coordinates':item['coordinates'],
                            'place':item['place'],
                            'quoted_status_id':int(item['quoted_status_id']) if 'quoted_status_id' in item else 0,
                            'quoted_status_id_str':item['quoted_status_id_str'] if 'quoted_status_id_str' in item else '',
                            'is_quote_status':item['is_quote_status'],
                            'retweeted_status':bool(item['retweeted_status']) if 'retweeted_status' in item else False ,
                            'reply_count':item['replay_count'] if 'replay_count' in item else 0,
                            'retweet_count':item['retweet_count']  if 'retweet_count' in item else 0,
                            'favorite_count':item['favorite_count'] if 'favorite_count' in item else 0,
                            'entities':item['entities'],
                            'extended_entities':item['extended_entities'] if 'extended_entities' in item else {},
                            'favorited':item['favorited'],
                            'retweeted':item['retweeted'],
                            'possibly_sensitive':item['possibly_sensitive'] if 'possibly_sensitive' in item else False,
                            'lang':item['lang'],
                            'matching_rules':item['matching_rules'] if 'matching_rules' in item else []

                        }
                        # print(item)
                        print(twitter_es_data['reply_count'],twitter_es_data['retweet_count'],twitter_es_data['favorite_count'])
                        data = json.dumps([twitter_es_data],indent=4)
                        # print(data)
                        result = self.asynchronous_request_facebook_api([{
                            'url':'http://59.110.52.213/stq/api/v1/pa/topicRowletTwitter/add',
                            'data':data
                        }])
                        print(result)
                        if bool(result[0].get("success", False)):
                            # item['es_pushed'] = True
                            update_doc = self.dbs.Tuser_post.find_one_and_delete({'_id': item['_id']})
                            print('更新了%s文档' % update_doc['_id'])
                        # except Exception as e:
                        #    raise e
                        conn.send(twitter_es_data['id'])
                except Exception as e:
                   print(e)
                   continue
    #name,id,current_location,birthday,category,fan_count,emails,hometown,link,location,website,likes.limit(3),new_like_count,about,description
    #导出facebook用户的所有文章
    def run_facebook_pusher(self,conn=None,project={}):
        # client = MongoClient()
        dbs = self.client['FaceBook']
        userSet = dbs['facebook']
        # with open(os.path.abspath('./facebook_user_ids.json'), 'r') as f:
        #     user_ids = json.load(f)
        # for id in user_ids['ids']:
        while True:
            post_count = self.dbs.Tuser_post.count({'site':'facebook',"update_status": True,'es_pushed':{'$exists':False}})
            print('\n')
            print('未推送到es的还有%s个\n' % post_count)
            # docs = self.dbs.Tuser_post.find({'site':'facebook','permalink_url':"/LeeMZeldin/videos/10155510361192701/"})
            if post_count==0:
                print('所有文章推送完毕')
                break;
            for item in self.dbs.Tuser_post.find({'site':'facebook',"update_status": True,'es_pushed':{'$exists':False}
                # "$or": [
                #     {"site": "facebook", "name": {"$regex": user['name']},'update_status':True,'es_pushed':False},
                #     {"site": "facebook", "name": user['name'],'update_status':True,'es_pushed':False},
                #     {"site": "facebook", "name": {"$regex": user["keywords"]},'update_status':True,'es_pushed':False}
                # ]
            }).limit(50):
                try:
                    user = userSet.find_one({"id": item['user_id']})
                    if not isinstance(item['create_at'],datetime):
                        item['create_at'] = datetime.strptime(item['create_at'], '%Y-%m-%d %H:%M').strftime(
                            '%Y-%m-%dT%H:%M:%S.000Z')
                    else:
                        item['create_at']=item['create_at'].strftime(
                            '%Y-%m-%dT%H:%M:%S.000Z')
                    item['index_name'] = 'rowlet_facebook_articles'
                    item['type_name'] = 'rowlet_facebook_articles'
                    # print(user)
                    #item['user']['create_at'] =
                    # print(user)
                    #name = re.sub(r"[\u4E00-\u9FA5]|[\u3040-\u30FF\u31F0-\u31FF]|[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7AF]|\\pP|\\pS",'', item['name'])
                    if 'likes_num' in user:
                        if type(user['likes_num'])==str:
                            user['likes_count']=int(user['likes_num'].replace(',','')) if not user['likes_num'].endswith('万') else int(user['likes_num'].replace('万','0000'))
                        else:
                            user['likes_count'] = user['likes_num']
                        del user['likes_num']

                    # print(user['likes_count'])
                    # print(item['comment_num'],item['likes_num'],item['share_count'])
                    user['_id']=str(user['_id'])
                    item['user'] = user
                    topick = list(map(lambda x: x.replace('#', ''), re.findall(r'#\s\S+|#\S+', item['message'])))
                    facebook_es_data={
                        'index_name':'rowlet_facebook_articles',
                        'type_name':'rowlet_facebook_articles',
                        'id':self.makeId(item['permalink_url'].replace('https://facebook.com','') if not item['permalink_url'].startswith('https') else item['permalink_url']),
                        'create_at':item['create_at'],
                        'user':item['user'],
                        'text':re.sub(r"[\u4E00-\u9FA5]|[\u3040-\u30FF\u31F0-\u31FF]|[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7AF]|[\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b]",'',item['message']),
                        'comment_num':int(item['comment_num']),
                        'likes_num':int(item['likes_num']),
                        'share_count':int(item['share_count']),
                        'last_untime':item['last_untime'],
                        'permalink_url':'https://facebook.com%s' % item['permalink_url'] if not item['permalink_url'].startswith('https') else item['permalink_url'],
                        'topick':list(map(lambda x:re.sub(r"[\u4E00-\u9FA5]|[\u3040-\u30FF\u31F0-\u31FF]|[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7AF]|\s|[-,.?:;\'\"!`]|(-{2})|(\.{3})|(\(\))|(\[\])|({}) ", '', x),topick))

                    }
                    # print(facebook_es_data)
                    data = json.dumps([facebook_es_data],indent=4)
                    print(facebook_es_data['comment_num'],facebook_es_data['likes_num'],facebook_es_data['share_count'])
                    result = self.asynchronous_request_facebook_api([{
                        'url': 'http://59.110.52.213/stq/api/v1/pa/topicRowletFacebook/add',
                        'data': data
                    }])
                    print('更新了%s用户' % facebook_es_data['id'])
                    print(result)
                    if bool(result[0].get("success", False)):
                        # item['es_pushed'] = True
                        update_doc = self.dbs.Tuser_post.find_one_and_update({'_id': item['_id']},
                                                                             {'$set': {'es_pushed': True}},
                                                                             return_document=ReturnDocument.AFTER)
                        print('更新了%s文档' % update_doc['_id'])
                    # continue
                    conn.send(facebook_es_data['id'])
                except Exception as e:
                    print(e)


    def twitter_pusher(self,item):
        if not isinstance(item['created_at'], datetime):
            item['created_at'] = datetime.strptime(item['created_at'], '%a %b %d %H:%M:%S %z %Y').strftime(
                '%Y-%m-%dT%H:%M:%S.000Z')
        else:
            item['created_at'] = item['created_at'].strftime(
                '%Y-%m-%dT%H:%M:%S.000Z')
        item['user']['created_at'] = datetime.strptime(item['user']['created_at'], '%a %b %d %H:%M:%S %z %Y').strftime(
            '%Y-%m-%dT%H:%M:%S.000Z')
        del item['user']['position']
        del item['user']['party']
        del item['user']['gender']
        del item['user']['birth']
        del item['user']['age']
        hashtags = item['entities']['hashtags'] if 'hashtags' in item['entities'] else []
        item['entities']['hashtags'] = list(map(lambda x: x['text'], hashtags)) if len(hashtags) != 0 else []
        # print(item['entities'])
        twitter_es_data = {
            'index_name': 'rowlet_twitter_articles',
            'type_name': 'rowlet_twitter_articles',
            'id': item['id'],
            'id_str': item['id_str'],
            'text': item['text'],
            'source': item['source'],
            'created_at': item['created_at'],
            'truncated': item['truncated'],
            'in_reply_to_status_id': item['in_reply_to_status_id'],
            'in_reply_to_status_id_str': item['in_reply_to_status_id_str'],
            'in_reply_to_screen_name': item['in_reply_to_screen_name'],
            'in_reply_to_user_id': item['in_reply_to_user_id'],
            'in_reply_to_user_id_str': item['in_reply_to_user_id_str'],
            'user': item['user'],
            'coordinates': item['coordinates'],
            'place': item['place'],
            'quoted_status_id': int(item['quoted_status_id']) if 'quoted_status_id' in item else 0,
            'quoted_status_id_str': item['quoted_status_id_str'] if 'quoted_status_id_str' in item else '',
            'is_quote_status': item['is_quote_status'],
            'retweeted_status': bool(item['retweeted_status']) if 'retweeted_status' in item else False,
            'reply_count': item['replay_count'] if 'replay_count' in item else 0,
            'retweet_count': item['retweet_count'] if 'retweet_count' in item else 0,
            'favorite_count': item['favorite_count'] if 'favorite_count' in item else 0,
            'entities': item['entities'],
            'extended_entities': item['extended_entities'] if 'extended_entities' in item else {},
            'favorited': item['favorited'],
            'retweeted': item['retweeted'],
            'possibly_sensitive': item['possibly_sensitive'] if 'possibly_sensitive' in item else False,
            'lang': item['lang'],
            'matching_rules': item['matching_rules'] if 'matching_rules' in item else []

        }
        # print(item)
        print(twitter_es_data['reply_count'], twitter_es_data['retweet_count'], twitter_es_data['favorite_count'])
        data = json.dumps([twitter_es_data], indent=4)
        # print(data)
        result = self.asynchronous_request_facebook_api([{
            'url': 'http://59.110.52.213/stq/api/v1/pa/topicRowletTwitter/add',
            'data': data
        }])
        print('Info:%s  更新了%s用户' % (datetime.now(), twitter_es_data['id']))
        print('Info:%s %s' % (datetime.now(), result))
        # if bool(result[0].get("success", False)):
        #     # item['es_pushed'] = True
        #     update_doc = self.dbs.Tuser_post.find_one_and_update({'_id': item['_id']}, {'$set': {'es_pushed': True}},
        #                                                          return_document=ReturnDocument.AFTER)
        #     print('更新了%s文档' % update_doc['_id'])


    def facebook_pusher(self,item):
        # client = MongoClient()
        dbs = self.client['FaceBook']
        userSet = dbs['facebook']
        # print(item)
        user = userSet.find_one({"id": str(item['user_id'])})
        if not isinstance(item['create_at'], datetime):
            item['create_at'] = datetime.strptime(item['create_at'], '%Y-%m-%d %H:%M').strftime(
                '%Y-%m-%dT%H:%M:%S.000Z')
        else:
            item['create_at'] = item['create_at'].strftime(
                '%Y-%m-%dT%H:%M:%S.000Z')
        item['index_name'] = 'rowlet_facebook_articles'
        item['type_name'] = 'rowlet_facebook_articles'

        if 'likes_num' in user:
            if type(user['likes_num']) == str:
                user['likes_count'] = int(user['likes_num'].replace(',', '')) if not user['likes_num'].endswith(
                    '万') else int(re.sub("\D",'',user['likes_num'].replace('万', '0000')))
            else:
                user['likes_count'] = user['likes_num']
            del user['likes_num']
        user['_id'] = str(user['_id'])
        user['update_time'] = user['update_time'].strftime(
                '%Y-%m-%dT%H:%M:%S.000Z')
        # if math.isnan(user['position']):
        del user['position']
        del user['party']
        del user['age']
        del user['birth']
        del user['gender']
        item['user'] = user
        topick = list(map(lambda x: x.replace('#', ''), re.findall(r'#\s\S+|#\S+', item['message'])))
        # print(item['permalink_url'].replace('https://facebook.com',''))
        facebook_es_data = {
            'index_name': 'rowlet_facebook_articles',
            'type_name': 'rowlet_facebook_articles',
            'id': self.makeId(item['permalink_url'].replace('https://facebook.com','') if not item['permalink_url'].startswith('https') else item['permalink_url']),
            'create_at': item['create_at'],
            'user': item['user'],
            'text': re.sub(
                r"[\u4E00-\u9FA5]|[\u3040-\u30FF\u31F0-\u31FF]|[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7AF]|[\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b]",
                '', item['message']),
            'comment_num': int(item['comment_num']),
            'likes_num': int(item['likes_num']),
            'share_count': int(item['share_count']),
            'last_untime': item['last_untime'],
            'permalink_url': 'https://facebook.com%s' % item['permalink_url'] if not item['permalink_url'].startswith('https') else item['permalink_url'],
            'topick': list(map(lambda x: re.sub(
                r"[\u4E00-\u9FA5]|[\u3040-\u30FF\u31F0-\u31FF]|[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7AF]|\s|[-,.?:;\'\"!`]|(-{2})|(\.{3})|(\(\))|(\[\])|({}) ",
                '', x), topick))

        }
        # print(facebook_es_data['create_at'])
        data = json.dumps([facebook_es_data], indent=4)
        # print(data)
        result = self.asynchronous_request_facebook_api([{
            'url': 'http://59.110.52.213/stq/api/v1/pa/topicRowletFacebook/add',
            'data': data
        }])
        print(facebook_es_data['comment_num'], facebook_es_data['likes_num'], facebook_es_data['share_count'])
        print('Info:%s  更新了%s用户' % (datetime.now(),facebook_es_data['id']))
        print('Info:%s  此文发表时间为%s' % (datetime.now(), facebook_es_data['create_at']))
        print('Info:%s %s' % (datetime.now(),result))


if __name__ == '__main__':
    # es = Espusher()
    # es.rsetStatus()
    #es.twitter_pusher()
    line = 'Congresswoman Alma S. Adams, PhD イベント'
    # print(re.sub(r"[\u4E00-\u9FA5]|[\u3040-\u30FF\u31F0-\u31FF]|[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7AF]", '', line))
    # line = '102万'
    print(re.sub(r"[\u4E00-\u9FA5]|[\u3040-\u30FF\u31F0-\u31FF]|[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7AF]|\\pP|\\pS",'', line))
    # es.facebook_pusher()