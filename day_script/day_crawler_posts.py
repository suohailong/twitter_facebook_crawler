# !/usr/bin/python3

# message = "Chloe was excited to see grandpa @realdonaldtrump this week in DC at The White House. I hope she gets to remember some of these once in a lifetime moments. # 外国"
# huati = re.findall(r'#\s\S+|#\S+',message)
# print(list(map(lambda x:x.replace('#',''),huati)))

import schedule
import datetime,time,sys,os
sys.path.append('.')
# print(sys)
from src.shedule import Shedule
from src.pkg.twitter.twitter import TWitter
from src.pkg.facebook.facebook_api import FaceBook
from history_data_back_scrpt.start_crawler_with_user_ids import crawler_init
import copy,json

def read_config():
    with open(os.path.abspath('config.json'), 'r') as f:
        app_config = json.load(f)
    return app_config

def facebook_every_day_job(dateline=None):
    s = Shedule()
    print("crawler facebook posts working...")
    crawler_init(name='facebook')
    if not dateline:
        dateline = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=3), '%Y-%m-%d')
        datetimeline = copy.deepcopy(dateline)
        s.crawler_tweets(FaceBook(), site='facebook', deadtime=datetimeline)
    else:
        s.crawler_tweets(FaceBook(), site='facebook', deadtime=dateline)
    print('crawler facebook posts finished')


schedule.every().day.at("10:29").do(facebook_every_day_job)

if __name__ == '__main__':
    config = read_config()
    facebook_every_day_job(dateline=config.get('deadtime',None))
    # print('<-----posts定时任务启动----->')
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)


