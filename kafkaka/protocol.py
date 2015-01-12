# coding: utf8

from kafkaka.bstruct import (
    Struct,
    ShortField, IntField, CharField, UnsignedCharField,
    LongLongField, UnsignedIntField, Crc32Field,
)


class KafkaProtocol(object):
    PRODUCE_KEY = 0
    FETCH_KEY = 1
    OFFSET_KEY = 2
    METADATA_KEY = 3
    OFFSET_COMMIT_KEY = 8
    OFFSET_FETCH_KEY = 9
    CLIENT_ID = 'kafkaka'


class HeadStruct(Struct):
    api_key = ShortField()
    api_version = ShortField(default=0)
    correlation_id = IntField()
    client_id_length = ShortField(default=len(KafkaProtocol.CLIENT_ID))
    client_id = CharField(default=KafkaProtocol.CLIENT_ID, length='client_id_length')


class MetaTopicStruct(Struct):
    topic_name_length = ShortField()
    topic_name = CharField(length='topic_name_length')


class MetaStruct(Struct):
    head = HeadStruct(api_key=KafkaProtocol.METADATA_KEY)
    topic_length = IntField(default=0)
    topics = MetaTopicStruct(repeat='topic_length')


class BrokerStruct(Struct):
    node_id = IntField()
    host_char_len = ShortField()
    host = CharField(length='host_char_len')
    port = IntField()


class PartitionStruct(Struct):
    error = ShortField()
    partition = IntField()
    leader = IntField()
    replicas_number = IntField()
    replicas = IntField(repeat='replicas_number')
    isr_number = IntField()
    isr = IntField(repeat='isr_number')


class TopicStruct(Struct):
    error = ShortField()
    topic_name_length = ShortField()
    topic_name = CharField(length='topic_name_length')
    partitions_number = IntField()
    partitions = PartitionStruct(repeat='partitions_number')


class MetaResponseStruct(Struct):
    correlation_id = IntField()
    brokers_number = IntField()
    brokers = BrokerStruct(repeat='brokers_number')
    topics_number = IntField()
    topics = TopicStruct(repeat='topics_number')


class MessageStruct(Struct):
    magic = UnsignedCharField(default=0)
    attributes = UnsignedCharField(default=0)
    key = IntField(default=-1)
    value_length = IntField()
    value = CharField(length='value_length')


class Crc32MessageStruct(Struct):
    crc = Crc32Field(source='message')
    message = MessageStruct()


class Crc32MessageStructWithLength(Struct):
    unknown = LongLongField(default=0)
    message_length = IntField(source='message')
    message = Crc32MessageStruct()


class MessageSetStruct(Struct):
    partition = IntField()
    message_set_length = IntField(source='message_set')
    message_set = Crc32MessageStructWithLength(repeat='message_set_length')


class ProducePayloadStruct(Struct):
    topic_name_length = ShortField()
    topic_name = CharField(length='topic_name_length')
    topic_payloads_number = IntField()
    topic_payloads = MessageSetStruct(repeat='topic_payloads_number')


class ProduceStruct(Struct):
    head = HeadStruct(KafkaProtocol.PRODUCE_KEY, 0)
    acks = ShortField(default=1)
    timeout = IntField(default=1000)
    payloads_number = IntField()
    payloads = ProducePayloadStruct(repeat='payloads_number')


class ResponsePartitionStruct(Struct):
    partition = IntField()
    error = ShortField()
    offset = LongLongField()


class ResponseTopicStruct(Struct):
    topic_name_length = ShortField()
    topic_name = CharField(length='topic_name_length')
    partitions_number = IntField()
    partitions = ResponsePartitionStruct(repeat='partitions_number')


class ProduceResponseStruct(Struct):
    correlation_id = IntField()
    topics_number = IntField()
    topics = ResponseTopicStruct(repeat='topics_number')


if __name__ == "__main__":

    s = HeadStruct(
        KafkaProtocol.METADATA_KEY,
        0,
        111,
    )
    print s.pack2bin()
    s2 = HeadStruct(
        KafkaProtocol.METADATA_KEY,
        0,
        112,
        client_id='ok'
    )
    print s2.pack2bin()
    m1 = MetaStruct(
        head=dict(correlation_id=111)
    )
    print m1.dump2nametuple()
    print m1.pack2bin()
    print m1._values

    m2 = MetaStruct(
        head=dict(correlation_id=222)
    )
    print m2.pack2bin()
    binary = m1.pack2bin()
    m3 = MetaStruct()
    print 'unpack'
    m3.unpack(binary)
    print m3.pack2bin()
    print m3.dump2nametuple()

    msg = MessageStruct(
        value='test'
    )
    print (msg.dump2nametuple(), msg.pack2bin())
    crc_msg = Crc32MessageStruct(
        message=dict(
            value='test'
        )
    )
    assert crc_msg.pack2bin() == 'Y*G\x87\x00\x00\xff\xff\xff\xff\x00\x00\x00\x04test'


    crc_msg_with_length = Crc32MessageStructWithLength(
        message=dict(
            message=dict(
                value='test'
            )
        )
    )
    assert crc_msg_with_length.pack2bin() == '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12Y*G\x87\x00\x00\xff\xff\xff\xff\x00\x00\x00\x04test'

    msg_set = MessageSetStruct(
        partition=1,
        message_set=[
            dict(
                message=dict(
                    message=dict(
                        value='test'
                    )
                )
            ),
        ]
    )
    msg_set.pack2bin()

    produce_msg = ProduceStruct(
        head = dict(correlation_id=1),
        payloads=[
            dict(
                topic_name='im-msg',
                topic_payloads=[
                    dict(
                        partition=0,
                        message_set=[
                            dict(
                                message=dict(
                                    message=dict(
                                        value='test'
                                    )
                                )
                            ),
                        ]
                    ),
                ],
            ),
        ]
    )
    assert produce_msg.pack2bin() == '\x00\x00\x00\x00\x00\x00\x00\x01\x00\x07kafkaka\x00\x01\x00\x00\x03\xe8\x00\x00\x00\x01\x00\x06im-msg\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x1e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12Y*G\x87\x00\x00\xff\xff\xff\xff\x00\x00\x00\x04test'
    out = ProduceStruct()
    out.unpack(produce_msg.pack2bin())
    print out.dump2nametuple()
    assert out.dump2nametuple().payloads[0].topic_name == 'im-msg'

    # test msgpack
    import msgpack
    p = msgpack.packb({'msg': 'hi', 'type': 4, 'dna': 470})
    print [p]
    print [ProduceStruct(
        head = dict(correlation_id=1),
        payloads=[
            dict(
                topic_name='im-msg',
                topic_payloads=[
                    dict(
                        partition=0,
                        message_set=[
                            dict(
                                message=dict(
                                    message=dict(
                                        value=p
                                    )
                                )
                            ),
                        ]
                    ),
                ],
            ),
        ]
    ).pack2bin()]
