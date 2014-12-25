# coding: utf8
from kafkaka.tornado_patch import KafkaClient
import tornado.ioloop

import time

if __name__ == "__main__":
    c = KafkaClient("t-storm1:9092", topic_names=['im-msg'])
    start = time.time()
    print ''
    for i in xrange(500):
        c.send_message('im-msg', u'你好', str(time.time()), i)
        c.send_message('im-msg', 'hi', str(time.time()), i)
    for i in xrange(500):
        c.send_message('im-msg', u'你好', str(time.time()), i)
        c.send_message('im-msg', 'hi', str(time.time()), i)
    print time.time() - start
    print 'this will not block'
    tornado.ioloop.IOLoop.instance().start()
