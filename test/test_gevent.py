# coding: utf8
from kafkaka.gevent_patch import KafkaClient
from gevent import joinall

import time

if __name__ == "__main__":
    c = KafkaClient("localhost:9092", topic_names=['im-msg'])
    start = time.time()
    all = []
    print ''
    for i in xrange(50):
        all.append(c.send_message('im-msg', u'你好'.encode('utf-8'), str(time.time()), str(i)))
        all.append(c.send_message('im-msg', 'hi', str(time.time()), str(i)))
    print 'this will not block'
    for i in xrange(50):
        all.append(c.send_message('im-msg', u'你好'.encode('utf-8'), str(time.time()), str(i)))
        all.append(c.send_message('im-msg', 'hi', str(time.time()), str(i)))
    joinall(all)
    print 'but this will block'
    print time.time() - start
