# coding: utf8
import itertools
from kafkaka.define import (
    DEFAULT_SOCKET_TIMEOUT_SECONDS,
    DEFAULT_KAFKA_PORT,
    DEFAULT_RETRY_TIMES,
)
from kafkaka.define import (
    KafkaError,
    ConnectionError,
    LeaderNotAvailable,
    UnknownTopicOrPartition,
    check_and_raise_error,
)
from kafkaka.conn import Connection as KafkaConnection
from kafkaka.protocol import (
    MetaStruct,
    MetaResponseStruct,
    ProduceStruct,
    ProduceResponseStruct,
)
import logging
from itertools import cycle
from time import sleep

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.WARNING)
log = logging.getLogger("kafka")


def initial_hosts(hosts):
    hosts = hosts.strip().split(',')
    r = []
    for host_with_port in hosts:
        res = host_with_port.split(':')
        host = res[0]
        port = int(res[1]) if len(res) > 1 else DEFAULT_KAFKA_PORT
        r.append((host.strip(), port))
    return r


class KafkaClient(object):
    CLIENT_ID = b"kafkaka"
    CORRELATION_SEED = itertools.count()

    def __init__(self,
                 hosts, client_id=CLIENT_ID,
                 timeout=DEFAULT_SOCKET_TIMEOUT_SECONDS,
                 retry_times=DEFAULT_RETRY_TIMES,
                 topic_names=[]):
        self.client_id = client_id
        self.timeout = timeout
        self.hosts = initial_hosts(hosts)
        self._retry_times = retry_times
        self.conns = {}
        self.brokers = {}
        self.topic_and_partition_to_brokers = {}
        self.topic_to_partitions = {}
        self.boot_metadata(topic_names)
        self.partitions_cycle = {}

    def _get_next_correlation_id(self):
        """
        Generate a new correlation id
        :return: correlation_id which is auto increase
        """
        return next(KafkaClient.CORRELATION_SEED)

    def _get_next_partition(self, topic_name):
        """
        round-robin pick one partion for topic
        :param topic_name:
        :return:
        """
        if topic_name not in self.partitions_cycle:
            partitions = self.topic_to_partitions[topic_name]
            self.partitions_cycle[topic_name] = cycle(partitions)
        return next(self.partitions_cycle[topic_name]).partition

    def _get_conn(self, host, port):
        """
        Get or create a connection to a broker using host and port
        :param host:
        :param port:
        :return:
        """
        host_key = (host, port)
        if host_key not in self.conns:
            self.conns[host_key] = KafkaConnection(
                host,
                port,
                timeout=self.timeout
            )
        return self.conns[host_key]

    def _load_metadata(self, metadata, expected_topics):
        log.debug('metadata %s', metadata)
        brokers = metadata.brokers
        for broker in brokers:
            self.brokers[broker.node_id] = broker
        log.debug('brokers %s', self.brokers)
        topics = metadata.topics
        for topic in topics:
            topic_name = topic.topic_name
            try:
                check_and_raise_error(topic)
            except (UnknownTopicOrPartition, LeaderNotAvailable) as e:
                if topic_name in expected_topics:
                    # The topic which you requested is not exist, raise!
                    raise
                log.error("Error loading topic metadata for %s: %s", topic_name, e)
                continue
            partitions = topic.partitions
            self.topic_to_partitions[topic.topic_name] = partitions
            for partition in partitions:
                # Check for partition errors
                # no leader for some of the partitions is acceptable
                try:
                    check_and_raise_error(partition)
                except LeaderNotAvailable:
                    log.error('No leader for topic %s partition %d', topic, partition)
                    continue
                self.topic_and_partition_to_brokers[(topic.topic_name, partition.partition)] = partition.leader

    def boot_metadata(self, expected_topics):
        """
        boot metadata from kafka server
        :param expected_topics: The topics to produce metadata for. If empty the request will yield metadata for all topics.
        :return:
        """
        correlation_id = self._get_next_correlation_id()
        _topics = [dict(topic_name=t) for t in expected_topics]
        request_bytes = MetaStruct(
            head=dict(correlation_id=correlation_id),
            topic_length=len(expected_topics),
            topics=_topics,
        ).pack2bin()
        for (host, port) in self.hosts*3:  # trick for auto-create topics
            try:
                with self._get_conn(host, port) as conn:
                    conn.send(request_bytes, correlation_id)
                    resp_bytes = conn.recv(correlation_id)
                resp = MetaResponseStruct()
                resp.unpack(resp_bytes)
                resp = resp.dump2nametuple()
                self._load_metadata(resp, expected_topics)
                return resp  # break the loop
            except ConnectionError as e:
                log.warning("Could not send request [%r] to server %s:%i, "
                            "trying next server: %s" % (correlation_id, host, port, e))
                continue
            except KafkaError as e:
                log.warning("Kafka metadata via request [%r] from server %s:%i is not available, "
                            "trying next server: %s" % (correlation_id, host, port, e))
                sleep(1)  # trick for auto-create topics
                continue
        raise KafkaError("All servers failed to process request")

    def send_message(self, topic_name, *msg):
        partition_id = self._get_next_partition(topic_name)
        correlation_id = self._get_next_correlation_id()
        request_bytes = ProduceStruct(
            head=dict(correlation_id=correlation_id),
            payloads=[
                dict(
                    topic_name=topic_name,
                    topic_payloads=[
                        dict(
                            partition=partition_id,
                            message_set=[dict(message=dict(message=dict(value=unicode(v).encode('utf8')))) for v in msg]
                        ),
                    ],
                ),
            ]
        ).pack2bin()
        node_id = self.topic_and_partition_to_brokers[(topic_name, partition_id)]
        broker = self.brokers[node_id]
        host, port = broker.host, broker.port
        for i in xrange(self._retry_times):
            with self._get_conn(host, port) as conn:
                try:
                    conn.send(request_bytes, correlation_id)
                except ConnectionError as e:
                    log.warning("Could not send request [%r] to server %s:%i, try again, %s" % (correlation_id, host, port, e))
                    if i == self._retry_times - 1:
                        log.error("Could not send request [%r] to server %s:%i, %s" % (correlation_id, host, port, e))
                    continue  # try more times
                try:
                    resp_bytes = conn.recv(correlation_id)
                    resp = ProduceResponseStruct()
                    resp.unpack(resp_bytes)
                    d = resp.dump2nametuple()
                    for topic in d.topics:
                        for partition in topic.partitions:
                            check_and_raise_error(partition)
                except ConnectionError as e:
                    log.error('Could not get response [%r] from server %s:%i, %s' % (correlation_id, host, port, e))
                except Exception as e:
                    log.error("Bad response [%r] from server %s:%i, %s" % (correlation_id, host, port, e))
            break