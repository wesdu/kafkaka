# coding: utf8
from gevent import monkey
from itertools import chain
import logging
from kafkaka.client import KafkaClient
from kafkaka.conn import Connection
from kafkaka.define import DEFAULT_POOL_SIZE
from gevent import spawn
from gevent.queue import Queue, Full
import socket
monkey.patch_socket()
log = logging.getLogger("kafka")


class Connection(Connection):

    def __init__(self, pool=None, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self._pool = pool

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.closed():
            self._pool.remove(self)
            self._pool = None
        else:
            self._pool.release(self)

    def close(self):
        super(Connection, self).close()


class ConnectionPool(object):
    def __init__(self, connection_class=Connection, pool_size=DEFAULT_POOL_SIZE, **connection_kwargs):
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.pool_size = pool_size  # the number of max parallel connections
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()
        self.reset()

    def __repr__(self):
        return "%s<%s%s>" % (
            type(self).__name__,
            self.connection_class.__name__,
            str(self.connection_kwargs)
        )

    def __len__(self):
        return len(self._available_connections) + len(self._in_use_connections)

    def reset(self):
        self.disconnect()
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()
        if self.pool_size is not None:
            self.q = Queue(maxsize=self.pool_size)
            for i in xrange(self.pool_size):
                self.q.put_nowait(1)
        else:
            self.q = None

    def get_connection(self):
        """
        Get a connection from the pool
        :return: Connection object
        """
        if self.q:
            self.q.get()
        try:
            try:
                c = self._available_connections.pop()
                log.debug('Get connection from pool %s' % (c,))
            except IndexError:
                c = self.connection_class(pool=self, **self.connection_kwargs)
            self._in_use_connections.add(c)
            return c
        except Exception as e:
            if self.q:
                self.q.put_nowait(1)
            raise e

    def release(self, connection):
        """
        Releases the connection back to the pool
        :param connection:
        :return:
        """
        if connection in self._in_use_connections:
            self._in_use_connections.remove(connection)
            self._available_connections.append(connection)
        if self.q:
            self.q.put_nowait(1)


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
        self._pool_size = kwargs.pop('pool_size', DEFAULT_POOL_SIZE)
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
            self._pools[key] = ConnectionPool(host=host, port=port, pool_size=self._pool_size)
        return self._pools[key].get_connection()

    def send_message(self, *args, **kwargs):
        return spawn(super(KafkaClient, self).send_message, *args, **kwargs)


if __name__ == "__main__":
    pass