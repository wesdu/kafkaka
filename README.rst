kafkaka
===============

a Kafka client which using easy described protocol tool, also be able to be used with Gevent and tornado.

WARNING: Under development, Now only support simple send method. Not support Python3.

USAGE
-------------------------

Install using pypi::

    pip install kafkaka

Install from source::

    git clone https://github.com/wesdu/kafkaka.git
    cd kafkaka
    python setup.py install

EXAMPLE
-------------------------

simple block mode::

    # coding: utf8
    from kafkaka.client import KafkaClient
    import time

    if __name__ == "__main__":
        c = KafkaClient("tx-storm1:9092")
        c.send_message('im-msg', 'hi', str(time.time()))
        c.send_message('im-msg', u'你好'.encode('utf8'), str(time.time()))
        print 'this will block'

using with Gevent::

    # coding: utf8
    from kafkaka.gevent_patch import KafkaClient
    from gevent import spawn
    from gevent import sleep
    import time

    if __name__ == "__main__":
        c = KafkaClient("t-storm1:9092", topic_names=['im-msg'])
        print ''
        for i in xrange(50):
            c.send_message('im-msg', u'你好'.encode('utf8'), str(time.time()), str(i))
            c.send_message('im-msg', 'hi', str(time.time()), str(i))
        print 'this will not block'
        for i in xrange(50):
            c.send_message('im-msg', u'你好'.encode('utf8'), str(time.time()), str(i))
            c.send_message('im-msg', 'hi', str(time.time()), str(i))
            sleep(0.1)
        print 'but this will block'
        sleep(30)

you can set the number of max parallel connections by using pool_size param::

    # coding: utf8
    from kafkaka.gevent_patch import KafkaClient
    from gevent import joinall

    import time

    if __name__ == "__main__":
        c = KafkaClient("t-storm1:9092",
                        topic_names=['im-msg'],
                        pool_size=10  # the number of max parallel connections.
        )
        start = time.time()
        all = []
        print ''
        for i in xrange(50):
            all.append(c.send_message('im-msg', u'你好'.encode('utf8'), str(time.time()), str(i)))
            all.append(c.send_message('im-msg', 'hi', str(time.time()), str(i)))
        print 'this will not block'
        for i in xrange(50):
            all.append(c.send_message('im-msg', u'你好'.encode('utf8'), str(time.time()), str(i)))
            all.append(c.send_message('im-msg', 'hi', str(time.time()), str(i)))
        joinall(all)
        print 'but this will block'
        print time.time() - start

using with tornado::

    # coding: utf8
    from kafkaka.tornado_patch import KafkaClient
    import tornado.ioloop

    import time

    if __name__ == "__main__":
        c = KafkaClient("t-storm1:9092", topic_names=['im-msg'])
        start = time.time()
        print ''
        for i in xrange(500):
            c.send_message('im-msg', u'你好'.encode('utf8'), str(time.time()), str(i))
            c.send_message('im-msg', 'hi', str(time.time()), str(i))
        for i in xrange(500):
            c.send_message('im-msg', u'你好'.encode('utf8'), str(time.time()), str(i))
            c.send_message('im-msg', 'hi', str(time.time()), str(i))
        print time.time() - start
        print 'this will not block'
        tornado.ioloop.IOLoop.instance().start()
