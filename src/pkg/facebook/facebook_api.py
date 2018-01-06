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
        self.crawler_reactions_queue = RedisQueue(name='facebook_reactions',redis_config=self.app_config['redis_config'])

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
                str1 = re.search(r'comments:{"\S{1,100}', str(html)).group() if re.search(r'comments:{"\S{1,100}',
                                                                                          str(html)) else 'count:0'
                comment = re.search(r'count:\d{1,}', str1).group()
                # print(share,likes,str1)
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
                result_list.append({
                    "url":item['url'],
                    "reactions":None
                })
        return result_list

    def make_next_page_url(self,url,page_id,next_time):
        # o = urlparse(url)
        # query_str = parse_qs(o.query)
        # for k, v in query_str.items():
        #     query_str[k] = v[0]
        # query_dict_cursor = json.loads(query_str['cursor'])
        # # print(query_str)
        # time_line = query_dict_cursor['timeline_cursor']
        # time_line_constructor = time_line.split(':')
        # time_line_constructor[2] = '00000000000%s' % next_time
        # time_line_constructor[4] = '%d' % (int(time_line_constructor[4]) - 20)
        # new_time_line = ':'.join(time_line_constructor)
        #
        # query_dict_cursor['timeline_cursor'] = new_time_line
        # query_str['cursor'] = query_dict_cursor
        # l_url = url.split('?')
        # l_url[1] = urlencode(query_str)
        # return '?'.join(l_url)
        default_next_page_ma = '09223372036854775788';
        return "https://www.facebook.com/pages_reaction_units/more/?page_id={0}&cursor=%7B%22timeline_cursor%22%3A%22timeline_unit%3A1%3A0000000000{1}%3A04611686018427387904%3A{2}%3A04611686018427387904%22%2C%22timeline_section_cursor%22%3A%7B%7D%2C%22has_next_page%22%3Atrue%7D&surface=www_pages_posts&unit_count=20&dpr=2&__user=0&__a=1".format(page_id,next_time,int(default_next_page_ma)-20)
    def crawler_reactions_nums(self,url):
        content = self.asynchronous_request(url)
        return self.__reactions_handler(content)

    def crawler_user_likes(self,url):

        content = self.asynchronous_request(url)
        return_list = []
        for item in content:
            # print(item['content'])
            likes_count = pq(item['content'])('._3xom').html()
            # print(likes_count)
            # if likes_count:
            #     people_likes_num = re.search(r'\d+,\d+,\d+',likes_count) if re.search(r'\d+,\d+,\d+',likes_count) else 0
            # else:
            #     people_likes_num=0;
            # print(people_likes_num)
            # print(likes_count)
            return_list.append({
                "url":item['url'],
                "like_count":likes_count if likes_count!=None else 0
            })
        return return_list;


    def fetch_user_tweets(self,id=None,deadline='2017-09-21',urls=[]):
        flag=True
        while True:
            try:
                content = self.asynchronous_request(urls)
                # print(content)
                if re.search(r'(/posts)',urls):
                    # print(content)
                    origin_html = content[0]['content']
                else:
                    origin = json.loads(content[0]['content'][9:])['domops']
                    origin_html = list(filter(lambda x: type(x) == dict, origin[0]))
                    # print(origin_html)
                    origin_html = origin_html[0]['__html']
                # print(origin_html)
                def scrape(i, e):
                    return {
                        "name": pq(e)('div.l_c3pyo2v0u div._6a._5u5j._6b>h5 a').text(),
                        "create_at": pq(e)('div.l_c3pyo2v0u div._6a._5u5j._6b>h5+div>span:nth-child(3) a>abbr').attr('title'),
                        "last_untime": pq(e)('div.l_c3pyo2v0u div._6a._5u5j._6b>h5+div>span:nth-child(3) a>abbr').attr(
                            'data-utime'),
                        "permalink_url": pq(e)('div.l_c3pyo2v0u div._6a._5u5j._6b>h5+div>span:nth-child(3) a').attr('href'),
                        "message": pq(e)('div.userContent').text() + pq(e)('div.mtm').text()
                    }
                _ = pq(origin_html)
                tweets = list(_('div._4-u2._4-u8').map(scrape))
                if(len(tweets)==0):
                    break;
                # print(tweets)
                tweet3 = []

                for x in filter(lambda x:x['create_at'],tweets):
                    # print(re.search(r'[年月日]',x['create_at']).group())
                    x['create_at']=re.sub(r'[年月日\(\)金木水火土]', ' ', x['create_at'])
                    x['create_at'] = datetime.strptime(x['create_at'], '%Y %m  %d    %H:%M').strftime('%Y-%m-%d %H:%M')
                    tweet3.append(x)

                def dedupe(items, key=None):
                    seen = set()
                    for item in items:
                        val = item if key is None else key(item)
                        if val not in seen:
                            yield item
                            seen.add(val)
                tweet3 = list(dedupe(tweet3, key=lambda d: (d['name'], d['create_at'],d['last_untime'],d['permalink_url'],d['message'])))
                urls=self.make_next_page_url(urls,id,tweet3[-1]['last_untime'])
                # reactions_urls = map(lambda x:'https://www.facebook.com%s' % x['permalink_url'],tweet3)
                # reactions = self.crawler_reactions_nums(reactions_urls)
                crawler_reactions_list = []
                for item in tweet3:
                    # print(item)
                    item['site']='facebook'
                    item['latest']='true'
                    item['update_status'] = False
                    # item['share_num'] = None  # reactions[0]['reactions']['share_count'] if reactions else 0
                    # item['likes_num'] = None  # reactions[0]['reactions']['likes_count'] if reactions else 0
                    # item['comment_num'] = None  # reactions[0]['reactions']['comment_count'] if reactions else 0
                    item['user_id'] = id
                    if deadline and tweet3.index(item)!= 0:
                        date = datetime.strptime(item['create_at'], '%Y-%m-%d %H:%M')
                        deadline_panduan = datetime.strptime('%s' % deadline, '%Y-%m-%d')
                        print((date - deadline_panduan).days)
                        if (date - deadline_panduan).days <= 0:
                            flag=False;
                            break;
                    # print(item['name'])
                    object_id = self.save(item)
                    crawler_reactions_list.append({'url':'https://facebook.com%s' % item['permalink_url'],'id':str(object_id)})
                    print('save %s ==>successfuly' % object_id)
                self.crawler_reactions_queue.put(crawler_reactions_list)
                if not flag or len(tweet3)<=1:
                    print('此用户的文章爬取完成')
                    break;
            except Exception as e:
                print(e)
                continue;

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

if __name__ == '__main__':
    facebook = FaceBook()
    facebook.crawler_reactions_nums('https://facebook.com/permalink.php?story_fbid=10155796394206704&id=101464786703')
    # result = facebook.crawler_user_likes('https://www.facebook.com/DonaldJTrumpJr/')
    # print(result)
    # facebook.fetch_user_tweets(id='295644160460352',urls='https://www.facebook.com/pages_reaction_units/more/?page_id=153080620724&cursor=%7B%22timeline_cursor%22%3A%22timeline_unit%3A1%3A00000000001509137254%3A04611686018427387904%3A9223372036854775768%3A04611686018427387904%22%2C%22timeline_section_cursor%22%3A%7B%7D%2C%22has_next_page%22%3Atrue%7D&surface=www_pages_posts&unit_count=20&dpr=2&__user=0&__a=1')

