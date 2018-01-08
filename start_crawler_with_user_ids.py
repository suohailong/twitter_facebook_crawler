from src.redis_helper import RedisQueue
import os,json, argparse

def read_config():
    with open(os.path.abspath('config.json'), 'r') as f:
        app_config = json.load(f)
    return app_config

def crawler_init(name='twitter'):
    print('<-----初始化程序----->')
    config = read_config()
    if name == 'twitter':
        twitter_crawler_queue = RedisQueue(name='twitter',redis_config=config['redis_config'])
        if twitter_crawler_queue.qsize()>0:
            twitter_crawler_queue.clear()
        if twitter_crawler_queue.qsize() == 0:
            with open(os.path.abspath('twitter_user_ids.json'), 'r') as f:
                user_ids = json.load(f)
                for id in user_ids['ids']:
                    twitter_crawler_queue.put(id)
        print('<-----有%s个任务需要完成----->',twitter_crawler_queue.qsize())
        print('<-----twitter初始化完成----->')
    else:
        facebook_crawler_queue = RedisQueue(name='facebook',redis_config=config['redis_config'])
        if facebook_crawler_queue.qsize()>0:
            facebook_crawler_queue.clear()
        if facebook_crawler_queue.qsize()==0:
            with open(os.path.abspath('facebook_user_ids.json'), 'r') as f:
                user_ids = json.load(f)
                for id in user_ids['ids']:
                    facebook_crawler_queue.put(id)
        print('<-----有%s个任务需要完成----->', facebook_crawler_queue.qsize())
        print('<-----facebook初始化完成----->')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-n', "--name",
                        help="push user",
                        default="twitter",
                        action="store")
    parser.add_argument('-a', "--all",
                        help="push all",
                        default=" ",
                        action="store")
    args = parser.parse_args()

    if args.all=='all':
        crawler_init(name='twitter')
        crawler_init(name='facebook')
    else:
        crawler_init(name=args.name)

