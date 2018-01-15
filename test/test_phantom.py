
import sys
import json
import pickle
import os
import requests
import logging
import time
import re

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def verification():
    cookies = pickle.load(open("cookies.txt", "rb"))
    s = requests.Session()
    for cookie in cookies:
        s.cookies.set(cookie['name'], cookie['value'])
    response = s.get("https://www.abc.com/account/list.aspx")
    bodyStr = response.text
    print(bodyStr)

def facebook(username, password, url):
    try:
        driver = webdriver.Firefox(executable_path="/Users/suohailong/phantomjs/geckodriver")
        #if not os.path.isfile('cookies.txt'):
        driver.get('https://www.facebook.com')
        driver.find_element_by_id('email').send_keys(username)
        driver.find_element_by_id('pass').send_keys(password)
        driver.find_element_by_id('login_form').submit()
            # driver.implicitly_wait(30)
            # cookies  = driver.get_cookies()
            # f = open('cookies.txt', 'wb')
            # pickle.dump(cookies, f)
            # f.close()
        # else:
        #     f = open('cookies.txt', 'rb')
        #     cookies = pickle.load(f)
        #     driver.get('https://www.facebook.com/?sk=welcome')
        #     for cookie in cookies:
        #         # print(type(cookie))
        #         print(cookie)
        #         cookie['domain'] = 'www%s' % cookie['domain']
        #         if 'expiry' in cookie:
        #             print(cookie['expiry'])
        #             cookie['expiry'] = int(cookie['expiry'])
        #         if re.match(r'_js_',cookie['name'],re.M|re.I):
        #             cookie['name']=re.sub(r'_js_','', cookie['name'])
        #
        #         driver.add_cookie(cookie)
        #     f.close()

        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "q"))
        )
        element = driver.find_element_by_name('q')
        element.send_keys("八月")
        element.submit()
        # 1.搜索出公司
        element = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "xt_uniq_2"))
        )
        btn = driver.find_element_by_id("xt_uniq_2")
        btn.click()
        #2.找到对应的公司
        #3.找到对应的公司后获取到对应主页的url，然后将url压入队列等待requests解析
        #4.requests访问到源码，然后找对应注释当中的内容解析成对应的数据。

    except Exception as e:
        logging.exception(e)
        #driver.close()
    else:
        print('成功')
        # driver.close()


if __name__ == '__main__':
    try:
        username = sys.argv[1]
        password = sys.argv[2]
        url = sys.argv[3]
    except IndexError:
        print('Usage: %s <username> <password> <url>' % sys.argv[0])
    else:
        facebook(username, password, url)
        #verification()

