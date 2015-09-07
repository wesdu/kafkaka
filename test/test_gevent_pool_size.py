
# coding: utf8
from kafkaka.gevent_patch import KafkaClient
from gevent import joinall
from gevent import spawn, sleep
import gevent
import time
from gevent.server import StreamServer
gevent.monkey.patch_all()



def test(c):
    i=0
    while 1:
        i += 1
        for j in xrange(0, 500):
            c.send_message('im-msg', u'你好'.encode('utf8')+" " + str(time.time()) + " " + str(i))
            c.send_message('im-msg', 'hi'.encode('utf8')+" " + str(time.time()) + " " + str(i))
        gevent.sleep(5)

if __name__ == "__main__":
    c = KafkaClient("localhost:9092",
                    topic_names=['im-msg'],
                    pool_size=3  # the number of max parallel connections.
    )
    spawn(test, c)
    def handle(socket, address): 
        socket.send("Hello from a telnet!\n") 
        for i in range(5): 
            socket.send(str(i) + '\n') 
        socket.close() 
    server = StreamServer(('127.0.0.1', 5000), handle) 
    server.serve_forever()
