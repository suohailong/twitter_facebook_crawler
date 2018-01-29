# !/usr/bin/python3

# message = "Chloe was excited to see grandpa @realdonaldtrump this week in DC at The White House. I hope she gets to remember some of these once in a lifetime moments. # 外国"
# huati = re.findall(r'#\s\S+|#\S+',message)
# print(list(map(lambda x:x.replace('#',''),huati)))

import schedule
import datetime,time,sys,os,json
sys.path.append('.')
from src.shedule import Shedule
# from src.pkg.twitter.twitter import TWitter
from src.pkg.facebook.facebook_api import FaceBook
from src.redis_helper import RedisQueue
# from history_data_back_scrpt.start_crawler_with_user_ids import crawler_init

def read_config():
    with open(os.path.abspath('config.json'), 'r') as f:
        app_config = json.load(f)
    return app_config

def crawler_facebook_init():
    config = read_config()
    print('<-----初始化程序----->')
    facebook_crawler_queue = RedisQueue(name='facebook_users', redis_config=config['redis_config'])
    if facebook_crawler_queue.qsize() > 0:
        print('<-----有%s个任务还未完成---->' % facebook_crawler_queue.qsize())
    if facebook_crawler_queue.empty():
        with open(os.path.abspath('facebook_user_ids.json'), 'r') as f:
            user_ids = json.load(f)
            for id in user_ids['ids']:
                facebook_crawler_queue.put(id)
    print('<-----有%s个任务需要完成----->' % facebook_crawler_queue.qsize())
    print('<-----facebook初始化完成----->')

def facebook_user_infos():
    s = Shedule()
    print("crawler facebook userInfo  working...")
    crawler_facebook_init()
    s.update_facebook_users_count(FaceBook())
    print('crawler facebook userInfo  finished')




schedule.every().day.at("08:30").do(facebook_user_infos)
# schedule.every().day.at("10:35").do(facebook_every_day_job)
#schedule.every().days.at("08:29").do(twitter_every_day_update_count_job)
#schedule.every().day.at("10:33").do(facebook_every_day_update_count_job)

if __name__ == '__main__':
    facebook_user_infos()
    # print('------定时任务facebook_crawler_userInfos-------')
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)


