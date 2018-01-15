
# !/usr/local/bin/python3.5
import asyncio
from aiohttp import ClientSession
import aiohttp
from time import time
from datetime import datetime
import re
import json
# import ssl
# ssl._create_default_https_context = ssl._create_unverified_context

async def request(url):
    print('request ===>: %s ' % url)
    async with ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
        async with session.get(url,proxy="http://127.0.0.1:51545") as response:
            response = await response.read()
            return response
            # print(response)

start = time()
loop = asyncio.get_event_loop()

#https://www.facebook.com/
tasks = [
    asyncio.ensure_future(request("https://graph.facebook.com/v2.11/6815841748_10155481331751749/comments?fields=id&summary=true&access_token=EAACEdEose0cBADTwUqSQTxXfbULFv1grvbvCLDZAoewGzcortECRTmZBRXTiv2XMw3Kq1Ur4t5fK72xZAIJDxq8qsfjH64AHE52JLzU0QZBMPN0v6Ss3PvojyopOGykHp6Wl40adtrTRD1bYFkFGEz86ZAGQdFNSpAXn23skyTsP6Q5qAQmtRcMXDGMAfZBcUxLNFRb3m6AabxZA055h3Js")),
    asyncio.ensure_future(request("https://graph.facebook.com/v2.11/6815841748_10155481331751749/likes?fields=id&summary=true&access_token=EAACEdEose0cBADTwUqSQTxXfbULFv1grvbvCLDZAoewGzcortECRTmZBRXTiv2XMw3Kq1Ur4t5fK72xZAIJDxq8qsfjH64AHE52JLzU0QZBMPN0v6Ss3PvojyopOGykHp6Wl40adtrTRD1bYFkFGEz86ZAGQdFNSpAXn23skyTsP6Q5qAQmtRcMXDGMAfZBcUxLNFRb3m6AabxZA055h3Js")),
    asyncio.ensure_future(request("https://graph.facebook.com/v2.11/6815841748_10155481331751749/reactions?fields=id&summary=true&access_token=EAACEdEose0cBADTwUqSQTxXfbULFv1grvbvCLDZAoewGzcortECRTmZBRXTiv2XMw3Kq1Ur4t5fK72xZAIJDxq8qsfjH64AHE52JLzU0QZBMPN0v6Ss3PvojyopOGykHp6Wl40adtrTRD1bYFkFGEz86ZAGQdFNSpAXn23skyTsP6Q5qAQmtRcMXDGMAfZBcUxLNFRb3m6AabxZA055h3Js"))
]
# tasks = [hello('https://www.facebook.com/'), hello('https://www.facebook.com/')]
loop.run_until_complete(asyncio.wait(tasks))
loop.close()
for task in tasks:
    print(json.loads(task.result()))
print(time()-start)



#
# def time_cmp(first_time ,second = '2017-01-01'):
#     first = re.match(r'\d{4}-\d{2}-\d{2}', first_time).group()
#     return datetime.strptime(first, '%Y-%m-%d') >= datetime.strptime(second, '%Y-%m-%d')
#
#
#
# dif = time_cmp('2017-12-16T16:00:00+0000')
# print(dif)


