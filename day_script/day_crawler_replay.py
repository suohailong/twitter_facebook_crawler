# !/usr/bin/python3

# message = "Chloe was excited to see grandpa @realdonaldtrump this week in DC at The White House. I hope she gets to remember some of these once in a lifetime moments. # 外国"
# huati = re.findall(r'#\s\S+|#\S+',message)
# print(list(map(lambda x:x.replace('#',''),huati)))

import schedule
import datetime,time,sys,os,json
sys.path.append('.')
from src.shedule import Shedule
from src.pkg.twitter.twitter import TWitter
from src.pkg.facebook.facebook_api import FaceBook
from src.redis_helper import RedisQueue

with open(os.path.abspath('config.json'), 'r') as f:
    app_config = json.load(f)

def twitter_every_day_update_count_job():
    s = Shedule()
    print("crawler twitter replay working...")
    s.crawler_tweets_replay()
    print('crawler twitter replay finished')



def check_queue_isEmpty():
    try:
        queue = RedisQueue(name='twitter_replay', redis_config=app_config['redis_config'])
        if queue.qsize()>0:
            twitter_every_day_update_count_job()
        else:
            print('[没有任务！！]')
        return
    except Exception as e:
        print(e)
        return;
schedule.every(3).minutes.do(twitter_every_day_update_count_job)


if __name__ == '__main__':
    print('<-----replay定时任务启动----->')
    # twitter_every_day_update_count_job()
    while True:
        schedule.run_pending()
        time.sleep(1)


