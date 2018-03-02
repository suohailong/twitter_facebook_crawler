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

pp = pprint.PrettyPrinter(indent=4)

def export_facebook_weipa_doc(db="FaceBook",collection="facebook",project={}):
    #"mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin"
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]

    with open('./twitter_weipa.json','r') as f :
        wp_ids = json.load(f)
    docs = []
    for item in wp_ids['facebook']:
        doc = collections.find_one({"id":item['_id']},{'_id':0,'id':1,'name':1})
        # del dict['_id']
        docs.append(doc)


    df2 = pd.DataFrame(docs)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                                   decode('utf-8') if isinstance(x, str) else x)
    print(len(docs))
    df2.to_excel('./export_data/%s/weipa/%s.xlsx' % ("facebook", docs[0]['name']), sheet_name='Sheet1')


def export_twitter_weipa_doc(db="Twitter",collection="twitter",project={}):
    #"mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin"
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]

    with open('./twitter_weipa.json','r') as f :
        wp_ids = json.load(f)
    docs = []
    for item in wp_ids['twitter']:
        doc = collections.find_one({"id_str":item['_id']},{'_id':0,'id_str':1,'name':1})
        # del dict['_id']
        docs.append(doc)


    df2 = pd.DataFrame(docs)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                                   decode('utf-8') if isinstance(x, str) else x)
    print(len(docs))
    df2.to_excel('./export_data/%s/weipa/%s.xlsx' % ("twitter", docs[0]['name']), sheet_name='Sheet1')



export_facebook_weipa_doc()
# export_twitter_weipa_doc()