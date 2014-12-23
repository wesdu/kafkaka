# coding: utf8
from gevent import monkey
from itertools import chain
import logging
from kafkaka.client import KafkaClient
from kafkaka.conn import Connection
from kafkaka.define import DEFAULT_POOL_SIZE
from gevent import spawn
from gevent.queue import Queue
monkey.patch_socket()
log = logging.getLogger("kafka")


class Connection(Connection):

    def __init__(self, pool=None, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self._pool = pool

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pool.release(self)


class ConnectionPool(object):
    def __init__(self, connection_class=Connection, pool_size=DEFAULT_POOL_SIZE, **connection_kwargs):
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        # below should be reset
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()
        self.q = Queue(maxsize=pool_size)
        for i in xrange(pool_size):
            self.q.put_nowait(i)

    def __repr__(self):
        return "%s<%s%s>" % (
            type(self).__name__,
            self.connection_class.__name__,
            str(self.connection_kwargs)
        )

    def __len__(self):
        return len(self._available_connections) + len(self._in_use_connections)

    def reset(self):
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

    def get_connection(self):
        """
        Get a connection from the pool
        :return: Connection object
        """
        self.q.get()
        try:
            c = self._available_connections.pop()
        except IndexError:
            c = self.connection_class(pool=self, **self.connection_kwargs)
        if c.closed():
            self.reset()
            return self.get_connection()
        self._in_use_connections.add(c)
        return c

    def release(self, connection):
        """
        Releases the connection back to the pool
        :param connection:
        :return:
        """
        self.q.put_nowait(1)
        if connection in self._in_use_connections:
            self._in_use_connections.remove(connection)
            self._available_connections.append(connection)


    def disconnect(self):
        """
        Disconnects all connections in the pool
        :return:
        """
        all_conns = chain(self._available_connections,
                          self._in_use_connections)
        for connection in all_conns:
            connection.disconnect()


class KafkaClient(KafkaClient):

    def __init__(self, *args, **kwargs):
        self._pools = {}
        super(KafkaClient, self).__init__(*args, **kwargs)

    def _get_conn(self, host, port):
        """
        Get or create a connection using Pool
        :param host: host name
        :param port: port number
        :return: Connection
        """
        key = (host, port)
        if key not in self._pools:
            self._pools[key] = ConnectionPool(host=host, port=port)
        return self._pools[key].get_connection()

    def _release(self, host, port, conn):
        key = (host, port)
        self._pools[key].release(conn)

    def boot_metadata(self, *args, **kwargs):
        super(KafkaClient, self).boot_metadata(*args, **kwargs)  # should be block

    def send_message(self, *args, **kwargs):
        return spawn(super(KafkaClient, self).send_message, *args, **kwargs)


if __name__ == "__main__":
    c = Connection('t-storm1')
    print repr(c)
    pool = ConnectionPool(connection_class=Connection, host="t-storm1")
    print repr(pool)