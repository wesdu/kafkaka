# -*- coding: utf-8 -*-
from distutils.core import setup
LONGDOC = u"""
## kafkaka
### INTRODUCE
a Kafka client which using easy described protocol tool - bstruct, also be able to be used with Gevent.

WARNNING: Under development, Now only support simple send method. Not support Python3.

### EXAMPLE
#### simple block mode
    from kafkaka.client import KafkaClient
    import time

    if __name__ == "__main__":
        c = KafkaClient("tx-storm1:9092")
        c.send_message('im-msg', 'hi', str(time.time()))
        c.send_message('im-msg', u'你好', str(time.time()))
        print 'this will block'

#### using with Gevent
    from kafkaka.gevent_patch import KafkaClient
    from gevent import spawn
    from gevent import sleep
    import time

    if __name__ == "__main__":
        c = KafkaClient("t-storm1:9092")
        print ''
        for i in xrange(50):
            c.send_message('im-msg', u'你好', str(time.time()), i)
            c.send_message('im-msg', 'hi', str(time.time()), i)
        print 'this will not block'
        for i in xrange(50):
            c.send_message('im-msg', u'你好', str(time.time()), i)
            c.send_message('im-msg', 'hi', str(time.time()), i)
            sleep(0.1)
        print 'but this will block'
        sleep(30)
"""

setup(name='kafkaka',
      version='0.1',
      description='Kafka Client with smarter protocol described and adapter for Gevent',
      long_description=LONGDOC,
      author='Du Wei',
      author_email='pandorid@gmail.com',
      url='https://github.com/wesdu/kafkaka',
      license="Apache License 2.0",
      classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Natural Language :: English',
        'Natural Language :: Chinese (Simplified)',
        'Natural Language :: Chinese (Traditional)',
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Topic :: Software Development :: Libraries :: Python Modules",
      ],
      keywords='Kafka,bstruct,gevent,client',
      packages=['kafkaka'],
      package_dir={'kafkaka':'kafkaka'},
      package_data={'kafkaka':['*.*']}, requires=['gevent']
)