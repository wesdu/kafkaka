# coding: utf8
from itertools import chain
import logging
import struct
import socket
import time
from collections import deque

from tornado import iostream
from tornado import gen

from kafkaka.define import DEFAULT_SOCKET_TIMEOUT_SECONDS, DEFAULT_RETRY_TIMES
from kafkaka.client import KafkaClient as BaseKafkaClient
from kafkaka.conn import Connection as BaseConnection
from kafkaka.define import KafkaError, ConnectionError, TopicError


logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG
)
log = logging.getLogger("kafka")


class Connection(BaseConnection):

    def __init__(self, pool=None, *args, **kwargs):
        self._ready = False
        super(Connection, self).__init__(*args, **kwargs)
        self._pool = pool
        self._stream = None
        self._callbacks = []
        self._work = deque()
        self._working = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pool.release(self)

    @gen.coroutine
    def connect(self):
        # this has to be replaced by a call to "start" so that the async is
        # properly handled
        pass

    @gen.coroutine
    def start(self):
        """
        Replaces `connect` for async operations.  The connection pool
        is now responsible not just for instantiation of a new connection
        but also of invoking "start" in some async context to guarnatee
        the connection is active before returing it.
        """
        try:
            if not self._ready:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
                logging.debug("connecting: %r", self)
                self._sock = iostream.IOStream(s)  # tornado iostream
                yield self._sock.connect((self._host, self._port))
                logging.debug("connected: %r", self)
                self._ready = True
        except (socket.error, socket.timeout):
            yield self.close()
            self._log_and_raise("Unable to connect to kafka broker")

    @gen.coroutine
    def do_work(self):
        """
        Requests and responses over the connection are serial, so keep all of
        the send and recv messages in the same place and orderly.

        This function guarantees there is only one sender and one reciever
        running at a time, and that the response is passed back into the
        proper future.
        """
        while not self._working and self._work:
            self._working = True
            try:
                payload, correlation_id, future = self._work.popleft()
                yield self.send(payload, correlation_id)
                res = yield self.recv(correlation_id)
                future.set_result(res)
            finally:
                self._working = False

    def communicate(self, payload, correlation_id=-1):
        """
        Entrypoint for bidirectional communication.

        :param payload: an encoded kafka packet
        :param correlation_id: for now, just for debug logging
        :return: kafka response packet.
        """
        future = gen.Future()
        self._work.append((payload, correlation_id, future))
        self.do_work()
        return future

    @gen.coroutine
    def send(self, payload, correlation_id=-1):
        """
        :param payload: an encoded kafka packet
        :param correlation_id: for now, just for debug logging
        :return:
        """
        log.debug(
            "About to send %d bytes to Kafka, request %d" %
            (len(payload), correlation_id)
        )
        if payload:
            _bytes = struct.pack('>i%ds' % len(payload), len(payload), payload)
        else:
            _bytes = struct.pack('>i', -1)
        try:
            res = yield self._sock.write(_bytes)
        except Exception:
            self.close()
            self._log_and_raise('Unable to send payload to Kafka')
        else:
            raise gen.Return(res)

    @gen.coroutine
    def recv_chunk(self, chunk_size):
        try:
            res = yield self._sock.read_bytes(min(chunk_size, 4096))
        except Exception:
            self.close()
            self._log_and_raise('Unable to receive data from Kafka')
        else:
            raise gen.Return(res)

    @gen.coroutine
    def recv(self, correlation_id=-1):
        """
        :param correlation_id: for now, just for debug logging
        :return: kafka response packet
        """
        log.debug("Reading response #%d from Kafka", correlation_id)
        resp = yield self.recv_chunk(4)
        log.debug(
            "Got %d bytes header back from Kafka for #%d: %s",
            len(resp) if resp else 0,
            correlation_id,
            "FAIL" if not resp else "SUCCESS"
        )
        if not resp:
            raise gen.Return(None)
        else:
            size, = struct.unpack('>i', resp)
            resp = yield self.recv_chunk(size)
            log.debug(
                "Got %d bytes response back from Kafka for #%d",
                len(resp) if resp else 0,
                correlation_id
            )
            raise gen.Return(resp)

    def close(self):
        log.debug("Closing socket connection" + self._log_tail)
        if self._sock:
            self._sock.close()
            self._sock = None
            self._ready = False
        else:
            log.debug("Socket connection not exists" + self._log_tail)

    def closed(self):
        return self._sock.closed()


class ConnectionPool(object):
    def __init__(self, connection_class=Connection, **connection_kwargs):
        self.retry_times = connection_kwargs.pop(
            "retry_times", DEFAULT_RETRY_TIMES
        )
        self.timeout = connection_kwargs.pop(
            "timeout", DEFAULT_SOCKET_TIMEOUT_SECONDS
        )
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
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
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

    @gen.coroutine
    def get_connection(self):
        """
        Get a connection from the pool
        :return: Connection object
        """
        try:
            c = self._available_connections.pop()
        except IndexError:
            c = self.connection_class(
                pool=self,
                timeout=self.timeout,
                **self.connection_kwargs
            )
            yield c.start()
        if c.closed():
            self.reset()
            c = yield self.get_connection()
        else:
            self._in_use_connections.add(c)
        raise gen.Return(c)

    def release(self, connection):
        """
        Releases the connection back to the pool
        :param connection:
        :return:
        """
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


class KafkaClient(BaseKafkaClient):

    def __init__(self, *args, **kwargs):
        self._pools = {}
        self._ready = False
        self._starting = None
        self.bad_topic_names = set()
        super(KafkaClient, self).__init__(*args, **kwargs)

    def get_connection(self, host, port):
        """
        Get or create a connection using Pool
        :param host: host name
        :param port: port number
        :return: Connection
        """
        key = (host, port)
        if key not in self._pools:
            self._pools[key] = ConnectionPool(
                host=host,
                port=port,
                retry_times=self._retry_times,
                timeout=self.timeout
            )
        return self._pools[key].get_connection()

    def boot_metadata(self, expected_topics, callback=None):
        # This is async and will generate a future which will start up as soon
        # as the ioloop starts up.
        # alternately, start() can (and should) be invoked as part of
        # initialization.
        self.start(expected_topics)

    def start(self, expected_topics=None):
        if not self._ready:
            if not self._starting:
                self._starting = self.fetch_metadata(expected_topics or [])
            return self._starting
        f = gen.Future()
        f.set_result(None)
        return f

    @gen.coroutine
    def _start(self):
        """
        Once the IOLoop starts, guaranteed to only do the fetch_metadata once
        to start, and will continue to retry for up to
        self._retry_times * self.timeout
        seconds at which point it'll raise a KafkaError.
        """
        if self._starting:
            timeout = self._retry_times * self.timeout
            start_time = time.time()
            while not self._ready:
                if time.time() - start_time > timeout:
                    raise KafkaError(
                        "Could not establish a connection to any backend"
                    )
                yield gen.sleep(.1)
        else:
            self._starting = True
            yield self.fetch_metadata(self.expected_topics)
            self.expected_topics = []

    @gen.coroutine
    def communicate(
        self, host, port, request_bytes, correlation_id, booting=False
    ):
        # we'll have to communicate once on the boot sequence, in which case
        # we'll not be ready.  Otherwise, probably best to check started.
        if not self._ready and not booting:
            yield self.start()

        conn = yield self.get_connection(host, port)
        with conn:
            resp_bytes = yield conn.communicate(
                request_bytes, correlation_id
            )
        if not resp_bytes:
            log.warning(
                "Got no response for [%r] to server %s:%i ",
                correlation_id, host, port
            )
        raise gen.Return(resp_bytes)

    @gen.coroutine
    def fetch_metadata(self, expected_topics, retry_wait=1, callback=None):
        """
        fetch boot metadata from kafka server.  This can be invoked
        multiple times in the case of a server configured to do
        topic auto-creation.
        :param expected_topics: The topics to produce metadata for.
            If empty the request will yield metadata for all topics.
        :return:
        """

        try:
            expected_topics = [str(t) for t in expected_topics]
        except UnicodeEncodeError:
            raise TopicError("Non-ascii topic name received: %r", t)

        correlation_id, _topics, request_bytes = self._pack_boot_metadata(
            expected_topics
        )

        # give the metadata fetch up self._retry_times tries on each known host
        for (host, port) in self.hosts * self._retry_times:
            try:
                resp_bytes = yield self.communicate(
                    host, port,
                    request_bytes, correlation_id,
                    booting=True
                )
                if resp_bytes:
                    self._unpack_boot_metadata(
                        resp_bytes, expected_topics, callback
                    )
                    if not self._ready:
                        self._ready = True
                        # we can't be starting if we're ready.  Clear that
                        self._starting = None
                    break
            except KafkaError as e:
                log.warning(
                    "Kafka metadata via request [%r] from server %s:%i "
                    "is not available, Retrying: %s",
                    correlation_id, host, port, e
                )
                # retry after a short sleep
                yield gen.sleep(retry_wait)
            except Exception as e:
                log.exception(
                    "Could not send request [%r] to server %s:%i, "
                    "trying next server", correlation_id, host, port
                )
        else:
            raise KafkaError("All servers failed to process request")

    @gen.coroutine
    def _pack_send_message(self, topic_name, *msg):
        if topic_name not in self.topic_to_partitions:
            log.warning(
                "Topic %r hasn't been seen.  Fetching metadata...",
                topic_name
            )
            try:
                yield self.fetch_metadata([topic_name])
            except TopicError:
                logging.exception("Couldn't auto-create topic!")
                self.bad_topic_names.add(topic_name)
                raise
            else:
                log.debug("Topic %s fetch has succeeded!", topic_name)

        raise gen.Return(
            super(KafkaClient, self)._pack_send_message(topic_name, *msg)
        )

    @gen.coroutine
    def send_message(self, topic_name, *msg, **kw):
        # because of the magic of async, we might fire send before
        # we connect,  Hold until ready
        while not self._ready:
            yield gen.sleep(.1)

        if topic_name in self.bad_topic_names:
            raise TopicError(
                "Can't write to topic %r because we failed previously" %
                topic_name
            )

        host, port, request_bytes, correlation_id = yield self._pack_send_message(
            topic_name, *msg
        )

        for i in xrange(self._retry_times):
            try:
                resp_byts = yield self.communicate(
                    host, port,
                    request_bytes, correlation_id
                )
            except ConnectionError as e:
                logger = log.warning
                if i == self._retry_times - 1:
                    logger = log.error
                logger(
                    "Could not communicate [%r] with server %s:%i, "
                    "Retry %d of %d",
                    correlation_id, host, port, i, self._retry,
                    exc_info=True
                )
            except Exception:
                log.exception(
                    "Bad response [%r] from server %s:%i" %
                    (correlation_id, host, port)
                )
            else:
                break
