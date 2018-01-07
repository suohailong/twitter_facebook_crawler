import sys,os,json,time
sys.path.append('.')
#print(sys.path)


from  src.redis_helper import  RedisQueue


if __name__ == '__main__':
    twitter_crawler_queue = RedisQueue(name='twittter')
    with open(os.path.abspath('twitter_user_ids.json'),'r') as f:
        user_ids = json.load(f)
        # print(user_ids)

        # print(twitter_crawler_queue.get_key())
        for id in user_ids['ids']:
            print(id)
            twitter_crawler_queue.put(id)
            time.sleep(1)
        # print(twitter_crawler_queue.qsize())
