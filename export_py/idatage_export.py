from pymongo import MongoClient,DESCENDING
import openpyxl
from openpyxl.workbook import Workbook
from openpyxl.writer.excel import ExcelWriter
import openpyxl.cell
from openpyxl.reader.excel import load_workbook
import pandas as pd
import numpy as np
import pprint

pp = pprint.PrettyPrinter(indent=4)


def exportDataFromMongoToXlsx(db="Twitter",collection="twitter",sortBy='followers_count',project={}):
    client = MongoClient()
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    pipline = [
        {"$group":{"_id":"$keywords"}}
    ]
    result = collections.aggregate(pipline)
    docs = []


    for item in list(result):
        query = {"keywords":item['_id']}
        doc = collections.find(query,project).sort(sortBy,DESCENDING).limit(3)
        for x in list(doc):
            x.pop('_id')
            x['twitter_url']='http://twitter.com/%s' % x['screen_name']
            # print(x)
            docs.append(x)
    # print(docs)
    df2 = pd.DataFrame(docs)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                                   decode('utf-8') if isinstance(x, str) else x)
    df2.to_excel('./export/%s/%s.xlsx' % ("twitter", "twitter"), sheet_name='Sheet1')
#name,id,current_location,birthday,category,fan_count,emails,hometown,link,location,website,likes.limit(3),new_like_count,about,description

# exportDataFromMongoToXlsx(
#     project={
#         # "_id:":False,
#         "name":True,
#         "screen_name":True,
#         "location":True,
#         "url":True,
#         "followers_count":True,
#         "friends_count":True,
#         "statuses_count":True,
#         "created_at":True,
#         "bySheet":True,
#         "keywords":True,
#        "display_url":True,
#         "listed_count":True,
#         "favourites_count":True,
#         "expanded_url":True,
#         "location":True,
#         "description":True,
#         "entities":True
#     }
# )

# exportDataFromMongoToXlsx(
#     db="FaceBook",
#     collection="facebook",
#     sortBy="fan_count",
#     project={
#         "id:":True,
#         "name":True,
#         "birthday":True,
#         "current_location":True,
#         "category":True,
#         "fan_count":True,
#         "emails":True,
#         "hometown":True,
#         "link":True,
#         "bySheet":True,
#         "keywords":True,
#         "website":True,
#         "likes":True,
#         "new_like_count":True,
#         "about":True,
#         "location":True,
#         "description":True
#     }
# )



def exportDataToXlsxAndFillNan():
    orignalDoc = pd.read_excel('./keywords.xlsx', sheet_name=None, index_col=None, na_values=['NA'])
    currentDoc = pd.read_excel('./export/facebook/facebook.xlsx', sheet_name=0, index_col=None, na_values=['NA'])
    currentDocList = set(pd.DataFrame(currentDoc)['keywords'])
    docList = []
    for item in orignalDoc.keys():
        df = pd.DataFrame(orignalDoc[item])
        if item=='The Cabinet':
            docList.extend(df['Name'])
        elif item=='House of Representatives':
            dataFrame = df.iloc[:,2:3]
            data = map(lambda x:''.join(x),dataFrame.values.tolist())
            docList.extend(list(data))
        elif item=='Governors':
            docList.extend(df['Governor'])
        else:
            docList.extend(df['Senator'])
    # print(docList)
    orignalDocList = set(docList)
    differenceData = list(orignalDocList.difference(currentDocList))
    ll = list(pd.DataFrame(currentDoc).to_dict('records'))
    def handeler(x):
        dic = {};
        dic["id"]=''
        dic["name"]=''
        dic["birthday"]=''
        dic["current_location"]=''
        dic["category"]=''
        dic["fan_count"]=''
        dic["emails"]=''
        dic["hometown"]=''
        dic["link"]=''
        dic["bySheet"]=x
        dic["keywords"]=''
        dic["website"]=''
        dic["likes"]=''
        dic["new_like_count"]=''
        dic["about"]=''
        dic["description"]=''
        dic["location"]=''
        return dic;
    # def handeler(x):
    #     dic = {};
    #     dic["name"]=''
    #     dic["screen_name"]=''
    #     dic["location"]=''
    #     dic["url"]=''
    #     dic["followers_count"]=''
    #     dic["friends_count"]=''
    #     dic["statuses_count"]=''
    #     dic["created_at"]=''
    #     dic["bySheet"]=''
    #     dic["keywords"]=x
    #     dic["display_url"]=''
    #     dic["listed_count"]=''
    #     dic["favourites_count"]=''
    #     dic["expanded_url"]=''
    #     dic["location"]=''
    #     dic["description"]=''
    #     dic["twitter_url"]=''
    #     return dic;
    suibian = list(map(handeler,differenceData))
    for ii in suibian:
        ll.append(ii)
    df2 = pd.DataFrame(ll)
    df2 = df2.applymap(lambda x: x.encode('unicode_escape').
                       decode('utf-8') if isinstance(x, str) else x)
    df2.to_excel('./export/%s/%s.xlsx' % ("facebook", "facebook"), sheet_name='Sheet1')



exportDataToXlsxAndFillNan()