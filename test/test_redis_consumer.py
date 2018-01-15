import sys,os,json
sys.path.append(".")

from  src.redis_helper import  RedisQueue


if __name__ == '__main__':
    twitter_crawler_queue = RedisQueue(name='twittter')
    while True:
        print(twitter_crawler_queue.qsize())
        print(twitter_crawler_queue.get())