# !/usr/bin/python3
# False,0,'',[],{},()都可以视为假
import pandas as pd
import numpy as np
import openpyxl
import requests
import json
from lxml import etree
from pyquery import PyQuery as pq

'https://www.facebook.com/pages_reaction_units/more/?page_id=780469468772262&cursor=%7B%22timeline_cursor%22%3A%22timeline_unit%3A1%3A00000000001513828536(这里代表上一条推文的时间错)%3A04611686018427387904%3A09223372036854775788(每访问一次这个数减20)%3A04611686018427387904%22%2C%22timeline_section_cursor%22%3A%7B%7D%2C%22has_next_page%22%3Atrue%7D&surface=www_pages_posts&unit_count=20&dpr=2&__user=0&__a=1'

content = requests.get('https://www.facebook.com/pages_reaction_units/more/?page_id=853073668057412&cursor=%7B%22timeline_cursor%22%3A%22timeline_unit%3A1%3A00000000001512855544%3A04611686018427387904%3A09223372036854775783%3A04611686018427387904%22%2C%22timeline_section_cursor%22%3A%7B%7D%2C%22has_next_page%22%3Atrue%7D&surface=www_pages_home&unit_count=20&dpr=2&__user=0&__a=1&__dyn=5V8WXBzagPxp2u6XolwCCwDKFbGEW8xdLFwgoqzob4q5UO5U4e2CEa-exebkwy6UnGiidz9XDG4Xze2y5ul0gKdxeu4oGqbAWCDxh1q7EO2S1iyECQ3e4oqyU9ooxqqVEgyk3GEtgWrwJxqawLh42ui2G262iu4rGUpCx65aBy9EixO12y9E9oKfzUy5uazrDwFxCQbUK8Lz-icK8Cx678-5E-8HgoUhwKl4ykby8cUSmh2osyo&__req=b&__be=-1&__pc=PHASED%3ADEFAULT&__rev=3543180')


origin = json.loads(content.text[9:])['domops']
origin_html = list(filter(lambda x:type(x)==dict, origin[0]))
origin_html = origin_html[0]['__html']



# print(origin_html)


# #解析文本
# html = etree.HTML(origin_html)
#
# #抽取信息
# element =html.xpath('//div[@class="_4-u2 _4-u8"]/*')
#
#
# print(element)
# print(element[0].items())

def scrape(i,e):
    return {
        "user_name":pq(e)('div.l_c3pyo2v0u div._6a._5u5j._6b>h5 a').text(),
        "create_time":pq(e)('div.l_c3pyo2v0u div._6a._5u5j._6b>h5+div>span:nth-child(3) a>abbr').attr('title'),
        "last_untime":pq(e)('div.l_c3pyo2v0u div._6a._5u5j._6b>h5+div>span:nth-child(3) a>abbr').attr('data-utime'),
        "permalink_url":pq(e)('div.l_c3pyo2v0u div._6a._5u5j._6b>h5+div>span:nth-child(3) a').attr('href'),
        "message":pq(e)('div.userContent').text()+pq(e)('div.mtm').text()

    }

_ = pq(origin_html)

doc = json.dumps(list(_('div._4-u2._4-u8').map(scrape)), ensure_ascii=False, indent=4)
print(doc)


