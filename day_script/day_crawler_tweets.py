# !/usr/bin/python3

# message = "Chloe was excited to see grandpa @realdonaldtrump this week in DC at The White House. I hope she gets to remember some of these once in a lifetime moments. # 外国"
# huati = re.findall(r'#\s\S+|#\S+',message)
# print(list(map(lambda x:x.replace('#',''),huati)))

import schedule,json
import datetime,time,sys,os
sys.path.append('.')
from src.shedule import Shedule
from src.pkg.twitter.twitter import TWitter
from src.pkg.facebook.facebook_api import FaceBook
from history_data_back_scrpt.start_crawler_with_user_ids import crawler_init

def read_config():
    with open(os.path.abspath('config.json'), 'r') as f:
        app_config = json.load(f)
    return app_config

def twitter_ervery_day_job(dateline=None):
    s = Shedule()
    print("crawler twitter tweets  working...")
    crawler_init(name='twitter')
    if not dateline:
        dateline = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=3),'%Y-%m-%d')
        s.crawler_tweets(TWitter(),'twitter',dateline)
    else:
        s.crawler_tweets(TWitter(), 'twitter', dateline)
    print('crawler twitter tweets finished')

schedule.every(2).hours.do(twitter_ervery_day_job)


if __name__ == '__main__':
    print('<-----tweets定时任务启动----->')
    # config = read_config()
    # twitter_ervery_day_job()
    while True:
        schedule.run_pending()
        time.sleep(1)


