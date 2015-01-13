# coding: utf8
from kafkaka.client import KafkaClient
import time

if __name__ == "__main__":
    c = KafkaClient("t-storm1:9092")
    start = time.time()
    print ''
    for i in xrange(50):
        c.send_message('im-msg', u'你好'.encode('utf8'), str(time.time()), str(i))
        c.send_message('im-msg', 'hi', str(time.time()), str(i))
    for i in xrange(50):
        c.send_message('im-msg', u'你好'.encode('utf8'), str(time.time()), str(i))
        c.send_message('im-msg', 'hi', str(time.time()), str(i))
    print time.time() - start
    print 'this will block'