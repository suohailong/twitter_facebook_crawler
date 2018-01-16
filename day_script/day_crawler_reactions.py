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



def facebook_every_day_update_count_job():
    s = Shedule()
    print("crawler facebook reactions  working...")
    s.crawler_reactions(FaceBook())
    print('crawler facebook reactions finished')

schedule.every(3).hours.do(facebook_every_day_update_count_job)

if __name__ == '__main__':
    print('<-----reactions定时任务启动----->')
    # facebook_every_day_update_count_job()
    while True:
        schedule.run_pending()
        time.sleep(1)


