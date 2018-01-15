# !/usr/bin/python3

# message = "Chloe was excited to see grandpa @realdonaldtrump this week in DC at The White House. I hope she gets to remember some of these once in a lifetime moments. # 外国"
# huati = re.findall(r'#\s\S+|#\S+',message)
# print(list(map(lambda x:x.replace('#',''),huati)))

import schedule
import datetime,time,sys,os
sys.path.append('.')
from src.shedule import Shedule
from src.pkg.twitter.twitter import TWitter
from src.pkg.facebook.facebook_api import FaceBook
from start_crawler_with_user_ids import crawler_init

def twitter_ervery_day_job():
    s = Shedule()
    print("crawler twitter tweets  working...")
    crawler_init(name='twitter')
    dateline = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=3),'%Y-%m-%d')
    s.crawler_tweets(TWitter(),'twitter',dateline)
    print('crawler twitter tweets finished')


def twitter_every_day_update_count_job():
    s = Shedule()
    print("crawler twitter replay working...")
    s.crawler_tweets_replay_count(TWitter())
    print('crawler twitter replay finished')


def facebook_every_day_job():
    s = Shedule()
    print("crawler facebook posts working...")
    # crawler_init(name='facebook')
    dateline = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=3), '%Y-%m-%d')
    s.crawler_tweets(FaceBook(), 'facebook',dateline)
    print('crawler facebook posts finished')


def facebook_every_day_update_count_job():
    s = Shedule()
    print("crawler facebook reactions  working...")
    s.crawler_reactions(FaceBook())
    print('crawler facebook reactions finished')



# schedule.every().day.at("08:30").do(twitter_ervery_day_job)
# schedule.every().day.at("10:35").do(facebook_every_day_job)
#schedule.every().days.at("08:29").do(twitter_every_day_update_count_job)
#schedule.every().day.at("10:33").do(facebook_every_day_update_count_job)

if __name__ == '__main__':

    while True:
        schedule.run_pending()
        time.sleep(1)


