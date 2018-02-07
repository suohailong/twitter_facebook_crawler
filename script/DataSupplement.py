import openpyxl,json
from openpyxl.workbook import Workbook
from openpyxl.writer.excel import ExcelWriter
import openpyxl.cell
from openpyxl.reader.excel import load_workbook
import pandas as pd
import numpy as np
from pymongo import MongoClient,DESCENDING

def isNaN(x):
    import math
    x = float('nan')
    return math.isnan(x)

def mongo_login(mnstr='127.0.0.1:27017',db='FaceBook',collection='facebook'):
    client = MongoClient(mnstr)
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    return collections

def read_excel(file,sheet,index):
    items = pd.read_excel('./%s.xlsx' % file, sheet, index_col=None, na_values=['NA'])
    df = pd.DataFrame(items)
    keyWordItems = df.values.tolist()[0:]
    # print(keyWordItems[0])
    return (item[index] for item in keyWordItems)

def reade_facebook_users_link(filename,sheet):
    total = 0;
    current = 0;
    facebook_ids = {
        'ids': []
    }
    fb_user_db = mongo_login()
    facebook_users_links= set(read_excel(filename, sheet, 10))|set(read_excel(filename, sheet, 11))
    facebook_iter = iter(facebook_users_links)
    for url in (item for item in facebook_iter if type(item)==str and item.startswith('https://')):
        user_doc = fb_user_db.find_one({'link':url})
        if user_doc:
            current += 1;
            facebook_ids['ids'].append(user_doc['id'])
        total += 1;
    with open('./facebook_user_ids2.json', 'w') as f:
        json.dump(facebook_ids, f, ensure_ascii=False, indent=4)
    print('fb总量：%s' % total)
    print('fb库中存在的量：%s' % current)

def reade_twitter_users_link(filename,sheet):
    total = 0;
    current = 0;
    twitter_ids = {
        'ids':[]
    }
    tw_user_db = mongo_login(db='Twitter',collection='twitter')
    twitter_users_links= set(read_excel(filename, sheet, 12))|set(read_excel(filename, sheet, 13))
    twitter_iter = iter(twitter_users_links)
    for url in (item for item in twitter_iter if type(item)==str and item.startswith('https://')):
        user_doc = tw_user_db.find_one({'screen_name':url.split('/')[3]})
        if user_doc:
            current += 1;
            twitter_ids['ids'].append(user_doc['id_str'])
        total += 1;
    with open('./twitter_user_ids2.json','w') as f:
        json.dump(twitter_ids, f, ensure_ascii=False, indent=4)
    print('tw总量：%s' % total)
    print('tw库中存在的量：%s' % current)


if __name__ == '__main__':
    reade_facebook_users_link('buchong','Sheet1')