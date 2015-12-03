# coding: utf8
import socket
import os
import struct
from nose.tools import nottest
from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from kafkaka.define import TopicError
from kafkaka.tornado_patch import KafkaClient
from . import mocket

# This unit test was developed in Docker with access to a kafka installation
# which has been linked to this container as `kafka`.
#
# Tests will still run normally without that, with the exception of
# live_fire_test which will require some special care (and also is the test
# used to get data for the other tests).
KAFKA_HOST = '{}:{}'.format(
    os.environ.get('KAFKA_PORT_9092_TCP_ADDR', "localhost"),
    os.environ.get('KAFKA_PORT_9092_TCP_PORT', 9092)
)


def to_kafka(payload):
    return struct.pack('>i%ds' % len(payload), len(payload), payload)


def add_no_topics_response(m):
    m.add_response(
        to_kafka(
            '\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x05'
            'kafka\x00\x00#\x84\x00\x00\x00\x00'
        )
    )


@nottest
def add_one_topic_test_response(m):
    m.add_response(
        to_kafka(
            (
                '\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x05'
                'kafka\x00\x00#\x84\x00\x00\x00\x01\x00\x00\x00\x04'
                'test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00'
                '\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00'
                '\x01\x00\x00\x00\x01'
            )
        )
    )
    return "test"


def add_no_such_topic_foo_response(m):
    m.add_response(
        to_kafka(
            '\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x05'
            'kafka\x00\x00#\x84\x00\x00\x00\x01\x00\x05\x00\x03foo'
            '\x00\x00\x00\x00'
        )
    )


def add_one_topic_foo_response(m):
    m.add_response(
        to_kafka(
            (
                '\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x05'
                'kafka\x00\x00#\x84\x00\x00\x00\x01\x00\x00\x00\x03'
                'foo\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00'
                '\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00'
                '\x01\x00\x00\x00\x01'
            )
        )
    )
    return "foo"


class KafkaProducerTestCase(AsyncTestCase):

    def assert_topics(self, client, topics):
        self.assertEqual(
            set(client.topic_to_partitions.keys()),
            set(topics)
        )
        for topic in topics:
            partitions = client.topic_to_partitions[topic]
            self.assertEqual(len(partitions), 1)
            partition = partitions[0]
            self.assertEqual(partition.error, 0)
            self.assertEqual(partition.partition, 0)
            self.assertEqual(partition.leader, 1)
            self.assertEqual(partition.replicas_number, 1)

    @nottest
    @gen_test
    def live_fire_test(self):
        with mocket.mocket(socket.AF_INET, socket.SOCK_STREAM, 0) as m:
            client = KafkaClient(KAFKA_HOST)
            yield client.start()
            yield client.send_message("foo", "bar")
            print client.topic_to_partitions
            import pprint
            pprint.pprint(m.get_transcript())
            assert False

    @nottest
    @gen_test
    def test_connection_fail(self):
        with mocket.mocket() as m:
            client = KafkaClient(KAFKA_HOST)
            # connect is async, so give it a chance to run.
            yield client.start()

    @gen_test
    def test_no_topics(self):
        with mocket.mocket() as m:
            # This says "oh hai, I don't have any topics"
            add_no_topics_response(m)

            client = KafkaClient(KAFKA_HOST)

            # connect is async, so give it a chance to run.
            yield client.start()

            self.assertTrue(client._ready)
            self.assertEqual(client.topic_to_partitions, {})

    @gen_test
    def test_no_topics_multi_start(self):
        with mocket.mocket() as m:
            # This says "oh hai, I don't have any topics"
            add_no_topics_response(m)

            client = KafkaClient(KAFKA_HOST)

            # start should be able to be run as many times as
            # we want
            for _ in range(3):
                yield client.start()
                yield gen.sleep(.5)

            self.assertTrue(client._ready)
            self.assertEqual(client.topic_to_partitions, {})

    @gen_test
    def test_topics(self):
        with mocket.mocket() as m:
            # This says "I have one topic named 'test'" with a
            # replication-factor" of 1 and 1 partition
            topic_name = add_one_topic_test_response(m)

            client = KafkaClient(KAFKA_HOST)

            # connect is async, so give it a chance to run.
            yield client.start()

            self.assertTrue(client._ready)
            # there should be a single topic
            self.assert_topics(client, [topic_name])

    @gen_test
    def test_topics_unicode(self):
        with mocket.mocket() as m:
            # This says "I have one topic named 'test'" with a
            # replication-factor" of 1 and 1 partition
            topic_name = add_one_topic_test_response(m)

            # Kafkaclient uses struct, and struct expects strings
            client = KafkaClient(
                KAFKA_HOST, topic_names=[unicode(topic_name)],
                timeout=1
            )

            # connect is async, so give it a chance to run.
            yield client.start()

            self.assertTrue(client._ready)
            # there should be a single topic
            self.assert_topics(client, [topic_name])

    @gen_test
    def test_topics_unicode_fail(self):
        with mocket.mocket() as m:
            # This says "I have one topic named 'test'" with a
            # replication-factor" of 1 and 1 partition
            topic_name = add_one_topic_test_response(m)

            # Kafkaclient uses struct, and struct expects strings
            try:
                client = KafkaClient(
                    KAFKA_HOST, topic_names=[u"touché"],
                    timeout=1
                )
                yield client.start()
            except TopicError:
                pass
            else:
                assert False, "Should have raised on non-ascii topic"
            self.assertFalse(client._ready)


    @gen_test
    def test_produce(self):
        with mocket.mocket() as m:
            topic_name = add_one_topic_test_response(m)

            client = KafkaClient(KAFKA_HOST)

            # connect is async, so give it a chance to run.
            yield client.start()
            self.assertTrue(client._ready)
            # there should be a single topic
            self.assert_topics(client, [topic_name])

            yield client.send_message(topic_name, topic_name)

            # that shouldn't have broken the client
            self.assertTrue(client._ready)
            self.assert_topics(client, [topic_name])

    @gen_test
    def test_multiproduce_bad_unicode(self):
        with mocket.mocket() as m:
            topic_name = add_one_topic_test_response(m)
            unicode_topic = u"touché"
            client = KafkaClient(KAFKA_HOST)

            # connect is async, so give it a chance to run.
            yield client.start()
            self.assertTrue(client._ready)
            # there should be a single topic
            self.assert_topics(client, [topic_name])

            try:
                yield client.send_message(unicode_topic, "test")
            except TopicError:
                pass
            else:
                assert False, "Should have raised on non-ascii topic"

            # the broken message shouldn't affect subsequent messages
            yield client.send_message(topic_name, "test")

            # that shouldn't have broken the client
            self.assertTrue(client._ready)
            self.assert_topics(client, [topic_name])

            # also we should have blacklisted this topic name
            self.assertTrue(client.bad_topic_names, set([unicode_topic]))

    @gen_test
    def test_multiproduce(self):
        with mocket.mocket() as m:
            topic_name = add_one_topic_test_response(m)

            client = KafkaClient(KAFKA_HOST)

            # connect is async, so give it a chance to run.
            yield client.start()
            self.assertTrue(client._ready)
            # there should be a single topic
            self.assert_topics(client, [topic_name])

            yield client.send_message(topic_name, topic_name)
            yield client.send_message(topic_name, topic_name)
            yield client.send_message(topic_name, topic_name)

            # that shouldn't have broken the client
            self.assertTrue(client._ready)
            self.assert_topics(client, [topic_name])

    @gen_test
    def test_produce_autocreate(self):
        with mocket.mocket() as m:
            # We're going to be responding as follows:
            #  1) I have a topic 'test' (metadata request)
            #  2) What's a 'foo'? [kafka autocreates, client retries]
            #  3) Oh right!  'foo'!  Here you go [success]
            topic_name = add_one_topic_test_response(m)
            add_no_such_topic_foo_response(m)
            new_topic = add_one_topic_foo_response(m)

            client = KafkaClient(KAFKA_HOST)

            # connect is async, so give it a chance to run.
            yield client.start()
            self.assertTrue(client._ready)
            # there should be a single topic
            self.assert_topics(client, [topic_name])

            yield client.send_message(new_topic, topic_name)

            # that shouldn't have broken the client
            self.assertTrue(client._ready)
            # There should now be 2 topics
            self.assert_topics(client, [topic_name, new_topic])

    @gen_test
    def test_produce_no_autocreate(self):
        with mocket.mocket() as m:
            # We're going to be responding as follows:
            #  1) I have a topic 'test' (metadata request)
            #  2) What's a 'foo'? [kafka autocreates, client retries]
            #  2') What's a 'foo'? [kafka autocreates, client retries]
            #  2'') What's a 'foo'? [kafka autocreates, client retries]
            #  3) client aborts.
            topic_name = add_one_topic_test_response(m)
            add_no_such_topic_foo_response(m)
            add_no_such_topic_foo_response(m)
            add_no_such_topic_foo_response(m)

            client = KafkaClient(KAFKA_HOST)

            # connect is async, so give it a chance to run.
            yield client.start()
            self.assertTrue(client._ready)
            # there should be a single topic
            self.assert_topics(client, [topic_name])

            try:
                yield client.send_message("foo", "test")
            except:
                pass
            else:
                assert False, "Should have raised a KafkaError"

            # that shouldn't have broken the client
            self.assertTrue(client._ready)
            # There should now be
            self.assert_topics(client, [topic_name])
