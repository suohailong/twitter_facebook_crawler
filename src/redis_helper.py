#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging

logger = logging.getLogger(__name__)

import redis, json



class RedisBase(object):
    def __init__(self, name, namespace='default', redis_config=None):
        if (not redis_config):
            redis_config = {'host': '127.0.0.1', 'port': 6379, 'db': 0}

        self.redis_config = redis_config
        self.__redis_connection = redis.StrictRedis(host=redis_config['host'], port=redis_config['port'],
                                                    db=redis_config['db'])
        self.password = redis_config.get('password', None)
        self.namespace = namespace
        self.name = name
        self.key = '%s:%s' % (self.namespace, self.name)
        self.__auth()

    def get_key(self):
        return self.key

    def __auth(self):
        if self.password:
            self.__redis_connection.execute_command("AUTH", self.password)

    def conn(self):
        self.__auth()
        # print(self.__redis_connection)
        return self.__redis_connection


class RedisQueue(RedisBase):
    def __init__(self, name, queue_type='lifo', namespace='queue', redis_config=None):
        super(RedisQueue, self).__init__(name, namespace=namespace, redis_config=redis_config)
        if (queue_type not in ['fifo', 'lifo']):
            raise Exception("queue_type has to be either fifo or lifo")
        self.queue_type = queue_type

    def qsize(self):
        """Return the approximate size of the queue."""
        return self.conn().llen(self.key)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0
    def lput(self,item):
        """Put item into the queue."""
        # print(json.dumps(item))
        self.conn().lpush(self.key, json.dumps(item))
    def put(self, item):
        """Put item into the queue."""
        # print(json.dumps(item))
        self.conn().rpush(self.key, json.dumps(item))

    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""
        if block:
            if (self.queue_type == 'fifo'):
                item = self.conn().blpop(self.key, timeout=timeout)
            elif (self.queue_type == 'lifo'):
                item = self.conn().brpop(self.key, timeout=timeout)
        else:
            if (self.queue_type == 'fifo'):
                item = self.conn().lpop(self.key)
            elif (self.queue_type == 'lifo'):
                item = self.conn().rpop(self.key)

        if item:
            item = json.loads(item[1].decode())
        return item

    def get_nowait(self):
        """Equivalent to get(False)."""
        return self.get(False)

    def clear(self):
        """Clear out the queue"""
        self.conn().delete(self.key)











