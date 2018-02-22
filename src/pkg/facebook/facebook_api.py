#!/usr/bin/python
#coding:utf-8
import sys
import os
import requests
import json
import logging
import re , operator,copy
import facebook,bs4, hashlib
import pandas as pd
from collections import Iterable
from datetime import datetime
from pyquery import PyQuery as pq
from urllib.parse import urlparse,parse_qs,urlunparse,urlencode
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common import action_chains


import sys, time, argparse, json, os, pprint

sys.path.append(".")
from src.pkg.base.base import Base
from src.redis_helper import RedisQueue
import aiohttp

class FaceBook(Base):
    """
    The RequestHandler class for our server.
    """
    def __init__(self,args={}):
        #Base.__init__()
        super(FaceBook,self).__init__()
        #以上是俩种调用基类构造函数的方法
        self.__username = "HenryAda1@inbox.ru"
        self.__password = "Is7eenLyWYg"
        self.__access_token = "EAACEdEose0cBAAkdhoyXkFejburMPqbr7b773AxZCs7b1BORK7V2gUxVlmKkYydZCZBuyy4UcZA0QxThf7ii0tbDnsiCSzwFJ9DZAeGTcUCsGHQPTk7hPamWAZA2mN6IBjNXDsDQwwzrwet4h1piWTP5fuBnKjZCGm8ZCyXjCEWS7apZCoo1ZAuO5OBfoc9IDCgjSDfvc3pWKWGEPcICelHO456OUnZAxeDpLUZD"
        self.__flag = 'facebook'
        self.args = args
        self.crawler_tweets_err_queue = RedisQueue(name='facebook_error', redis_config=self.app_config['redis_config'])
        self.crawler_reactions_queue = RedisQueue(name='facebook_reactions',redis_config=self.app_config['redis_config'])
        self.crawler_tweets_queue = RedisQueue(name='facebook',redis_config=self.app_config['redis_config'])
        self.facebook_users_queue = RedisQueue(name='facebook_users', redis_config=self.app_config['redis_config'])

    def __reactions_handler(self,responseText=[]):
        # print(responseText)
        if not responseText or (len(responseText) <= 0):
           return None
        result_list = []
        for item in responseText:
            try:
                bs = bs4.BeautifulSoup(item['content'], 'html.parser')
                if not bs:
                    continue;
                html = bs.select(
                    'script')
                share = re.search(r'sharecount:\d{1,}', str(html)).group() if re.search(r'sharecount:\d{1,}',
                                                                                        str(html)) else "sharecount:0"
                likes = re.search(r'likecount:\d{1,}', str(html)).group() if re.search(r'likecount:\d{1,}',
                                                                                       str(html)) else "likecount:0"
                comment = re.search(r'commentcount:\d{1,}', str(html)).group() if re.search(r'commentcount:\d{1,}',
                                                                                       str(html)) else "commentcount:0"
                # print(str1)
                # comment = re.search(r'count:\d{1,}', str1).group()
                # print(share,likes,comment)
                share_count = re.search(r'\d{1,}', share).group() if re.search(r'\d{1,}', share) else 0
                likes_count = re.search(r'\d{1,}', likes).group() if re.search(r'\d{1,}', likes) else 0
                comment_count = re.search(r'\d{1,}', comment).group() if re.search(r'\d{1,}', comment) else 0

                result_list.append({
                    "url": item["url"],
                    "reactions": {
                        "share_count": share_count,
                        "likes_count": likes_count,
                        "comment_count": comment_count
                    }
                })
            except Exception as e:
                # raise e
                result_list.append({
                    "url":item['url'],
                    "reactions":[]
                })
        return result_list

    def make_next_page_url(self,url,page_id,next_time,back_end=False):

        default_next_page_ma = '09223372036854775788';
        if back_end==1:
            return "https://www.facebook.com/pages_reaction_units/more/?page_id={0}&cursor=%7B%22timeline_cursor%22%3A%22timeline_unit%3A1%3A0000000000{1}%3A04611686018427387904%3A{2}%3A04611686018427387904%22%2C%22timeline_section_cursor%22%3A%7B%22profile_id%22%3A{3}%2C%22start%22%3A0%2C%22end%22%3A1517471999%2C%22query_type%22%3A36%2C%22filter%22%3A1%7D%2C%22has_next_page%22%3Atrue%7D&surface=www_pages_posts&unit_count=9&dpr=2&__user=0&__a=1&__req=j&__be=-1&__pc=EXP1:home_page_pkg&__rev=3574843".format(page_id,next_time,int(default_next_page_ma)-9,page_id)
        elif back_end==2:
            return "https://www.facebook.com/pages_reaction_units/more/?page_id={0}&cursor=%7B%22timeline_cursor%22%3A%22timeline_unit%3A1%3A0000000000{1}%3A04611686018427387904%3A{2}%3A04611686018427387904%22%2C%22timeline_section_cursor%22%3A%7B%22profile_id%22%3A{3}%2C%22start%22%3A1483257600%2C%22end%22%3A1514793599%2C%22query_type%22%3A8%2C%22filter%22%3A1%2C%22filter_after_timestamp%22%3A1487694945%7D%2C%22has_next_page%22%3Atrue%7D&surface=www_pages_posts&unit_count=8&dpr=2&__user=0&__a=1&__dyn=5V8WXBzamaUmgDxKS5o9FE9XGiWGey8jrWo466ES2N6xucxu13wFG2LzEjyR88xK5WAAzoOuVWxeUPwExnBg4bzojDx6aCyVeFFUkgmxGUO2S1iyECQ3e4oqyU9ooxqqVEgyk3GEtgWrwJxqawLh42ui2G262iu4rGUpCx65aBy9EixO12y9E9oKfzUy5uazrDwFxCibUK8Lz-icK8Cx6789E-8HgoUhwKl4ykby8cUSmh2osBK&__req=22&__be=-1&__pc=EXP1%3Ahome_page_pkg&__rev=3576820".format(
                page_id, next_time, int(default_next_page_ma) - 9,page_id)
        elif back_end==0:
            return "https://www.facebook.com/pages_reaction_units/more/?page_id={0}&cursor=%7B%22timeline_cursor%22%3A%22timeline_unit%3A1%3A0000000000{1}%3A04611686018427387904%3A{2}%3A04611686018427387904%22%2C%22timeline_section_cursor%22%3A%7B%7D%2C%22has_next_page%22%3Atrue%7D&surface=www_pages_posts&unit_count=9&dpr=2&__user=0&__a=1&__req=j&__be=-1&__pc=EXP1:home_page_pkg&__rev=3574843".format(page_id,next_time,int(default_next_page_ma)-9)
    def crawler_reactions_nums(self,url):
        try:
            content = self.asynchronous_request(url)
            return self.__reactions_handler(content)
        except Exception as e:
           raise e;

    def crawler_user_likes(self,url,user_id=None):
        try:
            content = self.asynchronous_request(url)
            return_list = []
            for item in content:
                # print(item['content'])
                user_community = pq(item['content'])('._3xom').text()
                print(user_community)
                if user_community == '0':
                    return_list.append({
                        "url": item['url'],
                        "like_count": user_community,
                        "fan_count": user_community
                    })
                elif user_community == '':
                    return_list.append({
                        "url": item['url'],
                        "isLoginStatus":True,
                        "like_count": '0',
                        "fan_count": '0'
                    })
                else:
                    if(len(user_community))>1:
                        if re.search(r'\s万',user_community):
                            likes_count,fan_count, = tuple(user_community.replace(' 万','0000').split(' '))
                        else:
                            likes_count, fan_count, = tuple(user_community.split(' '))
                        return_list.append({
                            "url": item['url'],
                            "isLoginStatus": True,
                            "like_count": likes_count,
                            "fan_count": fan_count
                        })
                    else:
                        # likes_count, fan_count, = tuple(user_community.split(' '))
                        return_list.append({
                            "url": item['url'],
                            "isLoginStatus": True,
                            "like_count": user_community,
                            "fan_count": 0
                        })
            return return_list;
        except aiohttp.ClientError as e:
            print('重新加入队列')
            self.facebook_users_queue.lput(user_id)
            return_list = []
            # if likes_count:
            #     people_likes_num = re.search(r'\d+,\d+,\d+',likes_count) if re.search(r'\d+,\d+,\d+',likes_count) else 0
            # else:
            #     people_likes_num=0;
            # print(people_likes_num)
            # print(likes_count)
            return return_list;

    def timestamp_to_strtime(self,timestamp):
        local_str_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        return local_str_time
    def fetch_user_tweets(self,id=None,deadline='2017-01-01',urls=[]):
        flag=True
        back=0
        while True:
            try:
                content = self.asynchronous_request(urls)
                if re.search(r'(/posts)',urls):
                    origin_html = content[0]['content']
                else:
                    origin = json.loads(content[0]['content'].decode()[9:])['domops']
                    origin_html = list(filter(lambda x: type(x) == dict, origin[0]))
                    origin_html = origin_html[0]['__html']
                def scrape(i, e):
                    return {
                        "name": pq(e)('div.userContentWrapper div._6a._5u5j._6b>h5 a').text(),
                        "create_at": pq(e)('div.userContentWrapper div._6a._5u5j._6b>h5+div>span:nth-child(3) a>abbr').attr('data-utime'),
                        "last_untime": pq(e)('div.userContentWrapper div._6a._5u5j._6b>h5+div>span:nth-child(3) a>abbr').attr(
                            'data-utime'),
                        "permalink_url": pq(e)('div.userContentWrapper div._6a._5u5j._6b>h5+div>span:nth-child(3) a').attr('href'),
                        "message": pq(e)('div.userContent p').text() + pq(e)('div.mtm div.mbs>a').text()
                    }

                _ = pq(origin_html)
                tweets = list(_('div._4-u2._4-u8').map(scrape))
                if(len(tweets)==0):
                    print('没有数据tweets为0')
                    break;
                # print(tweets)
                tweet3 = []
                printFlag = True;
                for x in filter(lambda x:x['create_at'],tweets):
                    # x['create_at']=re.sub(r'[年月日\(\)金木水火土]', ' ', x['create_at'])
                    # if printFlag:
                    #     print(x['create_at'])
                    #     printFlag=False
                    # thisTime = x['create_at']
                    # thisTime = thisTime.replace(',', '')
                    # thisTime = thisTime.replace('at', '')
                    # if 'am' in thisTime:
                    #     thisTime = thisTime.replace('am', ' AM')
                    # if 'pm' in thisTime:
                    #     thisTime = thisTime.replace('pm', ' PM')
                    # if 'Surday' in thisTime:
                    #     thisTime = thisTime.replace('Surday', 'Saturday')
                    # # # x['create_at'] = datetime.strptime(thisTime, '%A %B %d %Y  %H:%M %p').strftime('%Y-%m-%d %H:%M')
                    # x['create_at'] = datetime.strptime(thisTime, '%Y-%m-%d  %H:%M').strftime('%Y-%m-%d %H:%M') #最新修改
                    # x['create_at'] = datetime.strptime(x['create_at'], '%Y-%m-%d %H:%M').strftime('%Y-%m-%d %H:%M') #在本地跑数据
                    x['create_at']=self.timestamp_to_strtime(int(x['create_at']))
                    # print(x['create_at'])
                    tweet3.append(x)

                def dedupe(items, key=None):
                    seen = set()
                    for item in items:
                        val = item if key is None else key(item)
                        if val not in seen:
                            yield item
                            seen.add(val)
                tweet3 = list(dedupe(tweet3, key=lambda d: (d['name'], d['create_at'],d['last_untime'],d['permalink_url'],d['message'])))
                if len(tweet3)<=1:
                    back=back+1
                urls=self.make_next_page_url(urls,id,tweet3[-1]['last_untime'],back_end=back)
                # crawler_reactions_list = []
                for item in tweet3:
                    # print(item)
                    item['site']='facebook'
                    item['latest']='true'
                    item['update_status'] = False
                    item['update_time'] = datetime.today()
                    item['user_id'] = id
                    item['permalink_url'] = 'https://facebook.com%s' % item['permalink_url']
                    if deadline and tweet3.index(item)!= 0:
                        date = datetime.strptime(item['create_at'],'%Y-%m-%dT%H:%M:%S.000Z')
                        print(date)
                        deadline_panduan = datetime.strptime('%s' % deadline, '%Y-%m-%d')
                        print((date - deadline_panduan).days)
                        if (date - deadline_panduan).days <= 0:
                            flag=False;
                            break;
                    item['create_at'] = datetime.strptime(item['create_at'], '%Y-%m-%dT%H:%M:%S.000Z')
                    object_id = self.save(item)
                    # crawler_reactions_list.append({'url':item['permalink_url'],'id':str(object_id)})
                    print('save %s ==>successfuly' % object_id)
                # self.crawler_reactions_queue.put(crawler_reactions_list)
                print('获取的文档长度:%s' % len(tweet3))
                if not flag :
                    print('此用户的文章爬取完成')
                    back=0
                    break;
            except Exception as e:
                # print('<%s重新加载到文章队列>' % id)
                # self.crawler_tweets_err_queue.lput({'id':id,'url':urls})
                # # posts = self.get_mongod_client()
                # # deleteObj = posts.delete_many({'user_id':id})
                # # print('<清除%s用户的所有文章,文章数为:%s>' % (id,deleteObj.deleted_count))
                # break;
                raise e

    def searchUserInfo(self,keyword=[],typeIndex=1):
        print(keyword[typeIndex])
        self.__graph = facebook.GraphAPI(access_token=self.__access_token, version='2.10')
        kwInfo = self.__graph.search(type='page', q=keyword[int(typeIndex)])
        kInfos = kwInfo['data']
        if len(kInfos):
            for item in kInfos:
                res=self.__graph.get_object(item['id'],fields="name,id,current_location,birthday,category,fan_count,emails,hometown,link,location,website,likes.limit(3),new_like_count,about,description,verification_status")
                #friends = self.__graph.get_connections(id=item['id'], connection_name='friends')
                print(res['id'])
                res['keywords']=keyword[int(typeIndex)];
                # if int(typeIndex) == 2:
                #     res['searchBy'] = 'EnglishName'
                # else:
                #     res['searchBy'] = 'ChineseName'
                res['bySheet'] = self.args.sheet
                # print(super().save(res))
                id = super().save(res)
                if (id):
                    print('save %s==> ok' % id)
        else:
            print('没有数据')
        #super().saveAsExcel([],self.__flag,kw)
    def login(self):
        try:

            driver = webdriver.Firefox(executable_path="/Users/suohailong/phantomjs/geckodriver")
            driver.get('https://www.facebook.com')
            driver.find_element_by_id('email').send_keys(self.__username)
            driver.find_element_by_id('pass').send_keys(self.__password)
            driver.find_element_by_id('login_form').submit()
            element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "q"))
            )
            return driver
        except Exception as e:
            return False
    def getToken(self):
        facebookApiUrl = "https://developers.facebook.com/tools/explorer/145634995501895/?method=GET&path=168597536563870&version=v2.11"
        driver = self.login()
        if driver:
            driver.get(facebookApiUrl)
            element = WebDriverWait(driver, 10).until(
               EC.presence_of_element_located((By.XPATH, '//*[@id="facebook"]/body/div[2]/div[2]/div/div/div/div[2]/div/div[2]/a'))
            )
            actions = action_chains.ActionChains(driver)
            actions.click(element).perform()
            #menu = driver.find_element_by_xpath('//div[@class="uiContextualLayer uiContextualLayerBelowLeft"]/div/div/ul')
            getUserTokenItem = driver.find_element_by_xpath('//div[@class="uiContextualLayer uiContextualLayerBelowLeft"]/div/div/ul/li[1]/a')
            getUserTokenItem.click()
            tokenButton = driver.find_element_by_xpath('//*[@id="facebook"]/body/div[8]/div[2]/div/div/div/div/div[3]/div/div/div[2]/div/div/button[1]')
            tokenButton.click()
            tokenIput = driver.find_element_by_xpath('//*[@id="facebook"]/body/div[2]/div[2]/div/div/div/div[2]/div/div[1]/label/input')
            self.__access_token=tokenIput.get_attribute('value')
            print(self.__access_token)
            driver.quit()
            return True
        else:
            return False
    def getPagePosts(self):
        pass;

    def search_users(self, keyword='',typeIndex=1):
        try:
            print('当前参数为:%s' % keyword)
            self.searchUserInfo(keyword,typeIndex)
        except Exception as e:
            if e.code == 190:
                print('access Token has expired =====>Reget Touken!')
                while self.getToken():
                    self.searchUserInfo(keyword, typeIndex)
                    break;

            #logging.exception(e)

    def get_user_info(self,url):
        content = self.asynchronous_request(url)
        origin_html = content[0]['content']
        # print(content)
        _ = pq(origin_html)
        # print(_('#content_container div.clearfix').text())

        id = re.search(r'PagesProfileAboutInfoPagelet_\d+',origin_html.decode())
        id = id.group()
        name = re.sub(
                r"[\u4E00-\u9FA5]|[\u3040-\u30FF\u31F0-\u31FF]|[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7AF]|[-,.?:;\'\"!`]|(-{2})|(\.{3})|(\(\))|(\[\])|({})",
                '', _('#pageTitle').text())
        birthday = _('#content_container div.clearfix').text()
        website = _('#content_container div.clearfix').text()
        origin_str = _('#content_container div.clearfix').text()

        # print(origin_str)

        if re.search(r'(\d+)年(\d+)月(\d+)日',birthday):
            birthday = re.search(r'(\d+)年(\d+)月(\d+)日',birthday).group()
            birthday = re.sub(r'(\d+)年(\d+)月(\d+)日',r'\1-\2-\3',birthday)
            birthday =  re.sub(
                    r"[\u4E00-\u9FA5]|[\u3040-\u30FF\u31F0-\u31FF]|[\u1100-\u11FF\u3130-\u318F\uAC00-\uD7AF]|\s|[.?:;\'\"!`]|(-{2})|(\.{3})|(\(\))|(\[\])|({})",
                    '',birthday)
        else:
            birthday=''
        verified = re.search(
            r'Facebook \\u5df2\\u786e\\u8ba4\\u8fd9\\u662f\\u516c\\u4f17\\u4eba\\u7269\\u3001\\u5a92\\u4f53\\u516c\\u53f8\\u6216\\u54c1\\u724c\\u7684\\u771f\\u5b9e\\u4e3b\\u9875',
            origin_html.decode())
        if verified:
            verified = True
        else:
            verified=False

        item = self.crawler_user_likes(url.replace('/about','')+'/community/')
        if re.search(r'((http|https)://)[\w1-9]+.[\w1-9]+.*',website):
            website = re.search(r'((http|https)://)[\w1-9]+.[\w1-9]+.[\w]+',website).group()
        else:
            website = ''
        user_id = self.save_user(doc={
            "id": re.search(r'\d+',id).group(),
            "birthday": birthday,
            "link": url.replace('/about',''),
            'website': website,
            # 'about':re.search(r'简介 (\w+\s)+.',origin_str).group().replace('简介','') if re.search(r'简介 (\w+\s)+.',origin_str) else '',
            'about': _('div.text_exposed_root').text(),
            'hometown':re.search(r'来自 (\S+\s)+简介',origin_str).group().replace('来自','').replace('简介','') if re.search(r'来自 (\S+\s)+简介',origin_str) else '',
            'name': name.replace("Facebook","").replace('|','') ,
            'gender':re.search(r'性别 \S',origin_str).group().replace('性别','') if re.search(r'性别 \S',origin_str) else '',
            'PoliticalViews':re.search(r'政治观点 \S+\s',origin_str).group().replace('政治观点','') if re.search(r'政治观点 \S+\s',origin_str) else '',
            'ReligiousBeliefs':re.search(r'宗教信仰 \S+\s',origin_str).group().replace('宗教信仰','') if re.search(r'宗教信仰 \S+\s',origin_str) else '',
            'category':re.search(r'categories \S+\s',origin_str).group().replace('categories','') if re.search(r'categories \S+\s',origin_str) else '',
            'fan_count':item[0].get('fan_count',0),
            'likes_num':item[0].get('like_count',0),
            'verified':verified
        },dbName='FaceBook',collectionName='facebook')
        print("[===存储%s成功===]" % user_id)



if __name__ == '__main__':
    facebook = FaceBook()
    # with open('facebook_user_ids3.json','r') as f:
    #     ids = json.load(f)
    # current=0
    # for url in ids['ids']:
    # # facebook.crawler_reactions_nums('https://www.facebook.com/MakeAmericaProud/posts/1884405568288166')
    #     user_info = facebook.get_user_info(url+'about')
    #     current += 1;
    #     print('完成到第%s个' % current)
    #     # print(user_info)
    # # print(result)
    facebook.fetch_user_tweets(id='397176447066236',urls='https://www.facebook.com/pg/RepCarolynMaloney/posts/')
