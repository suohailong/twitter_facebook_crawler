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

def create_mongo_conn(mnStr='mongodb://127.0.0.1:27017',db="Twitter",collection="twitter"):
    print('链接==》%s' % mnStr)
    client = MongoClient(mnStr)
    dbs = client['%s' % db]
    collections = dbs['%s' % collection]
    return collections


def export_localDB_to_remoteDB():
    cli_1 = create_mongo_conn()
    cli_2 = create_mongo_conn(mnStr="mongodb://root:joke123098@101.201.37.28:3717/?authSource=admin")

    result = True;
    i = 0;
    while result:
        result = list(cli_1.find({}).skip(i).limit(300))
        # print(result)
        try:
            o = cli_2.insert_many(result)
            print(o.inserted_ids)
        except Exception as e:
            print(e)

        i+=300




if __name__ == "__main__":
    export_localDB_to_remoteDB()