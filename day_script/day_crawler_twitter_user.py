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
# from history_data_back_scrpt.start_crawler_with_user_ids import crawler_init
from src.redis_helper import RedisQueue

def read_config():
    with open(os.path.abspath('config.json'), 'r') as f:
        app_config = json.load(f)
    return app_config

def crawler_twitter_init():
    config = read_config()
    print('<-----初始化程序----->')
    twitter_crawler_queue = RedisQueue(name='twitter_users', redis_config=config['redis_config'])
    if twitter_crawler_queue.qsize() > 0:
        print('<-----有%s个任务还未完成---->' % twitter_crawler_queue.qsize())
    if twitter_crawler_queue.empty():
        with open(os.path.abspath('twitter_user_ids.json'), 'r') as f:
            user_ids = json.load(f)
            for id in user_ids['ids']:
                twitter_crawler_queue.put(id)
    print('<-----有%s个任务需要完成----->' % twitter_crawler_queue.qsize())
    print('<-----twitter初始化完成----->')


def twitter_user_infos():
    s = Shedule()
    print("crawler twitter userInfo  working...")
    crawler_twitter_init()
    s.update_twitter_users_count(TWitter())
    print('crawler twitter userInfo  finished')




schedule.every().day.at("08:30").do(twitter_user_infos)
# schedule.every().day.at("10:35").do(facebook_every_day_job)
#schedule.every().days.at("08:29").do(twitter_every_day_update_count_job)
#schedule.every().day.at("10:33").do(facebook_every_day_update_count_job)

if __name__ == '__main__':
    twitter_user_infos()
    # print('------定时任务facebook_crawler_userInfos-------')
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)


