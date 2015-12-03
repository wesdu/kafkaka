# coding: utf8

#
# config
#

DEFAULT_SOCKET_TIMEOUT_SECONDS = 30
DEFAULT_KAFKA_PORT = 9092
DEFAULT_RETRY_TIMES = 3
DEFAULT_POOL_SIZE = None

#
# custom errors
# see: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes


class KafkaError(Exception):
    pass


class TopicError(Exception):
    pass


class ConnectionError(KafkaError):
    """
    socket connection error
    """


class NoError(KafkaError):
    """
    No error--it worked!
    """
    code = 0


class Unknown(KafkaError):
    """
    An unexpected server error
    """
    code = -1


class OffsetOutOfRange(KafkaError):
    """
    The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
    """
    code = 1


class InvalidMessage(KafkaError):
    """
    This indicates that a message contents does not match its CRC
    """
    code = 2


class UnknownTopicOrPartition(KafkaError):
    """
    This request is for a topic or partition that does not exist on this broker.
    """
    code = 3


class InvalidMessageSize(KafkaError):
    """
    The message has a negative size
    """
    code = 4


class LeaderNotAvailable(KafkaError):
    """
    This error is thrown if we are in the middle of a leadership election and
    there is currently no leader for this partition and hence it is unavailable for writes.
    """
    code = 5


class NotLeaderForPartition(KafkaError):
    """
    This error is thrown if the client attempts to send messages to a replica that
    is not the leader for some partition. It indicates that the clients metadata is out of date.
    """
    code = 6


class RequestTimedOut(KafkaError):
    """
    This error is thrown if the request exceeds the user-specified time limit in the request.
    """
    code = 7


class BrokerNotAvailable(KafkaError):
    """
    This is not a client facing error and is used mostly by tools when a broker is not alive.
    """
    code = 8


class ReplicaNotAvailable(KafkaError):
    """
    If replica is expected on a broker, but is not.
    """
    code = 9


class MessageSizeTooLarge(KafkaError):
    """
    The server has a configurable maximum message size to avoid unbounded memory allocation.
    This error is thrown if the client attempt to produce a message larger than this maximum.
    """
    code = 10


class StaleControllerEpochCode(KafkaError):
    """
    Internal error code for broker-to-broker communication.
    """
    code = 11


class OffsetMetadataTooLargeCode(KafkaError):
    """
    If you specify a string larger than configured maximum for offset metadata
    """
    code = 12


class OffsetsLoadInProgressCode(KafkaError):
    """
    The broker returns this error code for an offset fetch request
    if it is still loading offsets (after a leader change for that offsets topic partition).
    """
    code = 14


class ConsumerCoordinatorNotAvailableCode(KafkaError):
    """
    The broker returns this error code for consumer metadata requests
    or offset commit requests if the offsets topic has not yet been created.
    """
    code = 15


class NotCoordinatorForConsumerCode(KafkaError):
    """
    The broker returns this error code if it receives an offset fetch
    or commit request for a consumer group that it is not a coordinator for.
    """
    code = 16


error_codes = {
    -1: Unknown,
    0: NoError,
    1: OffsetOutOfRange,
    2: InvalidMessage,
    3: UnknownTopicOrPartition,
    4: InvalidMessageSize,
    5: LeaderNotAvailable,
    6: NotLeaderForPartition,
    7: RequestTimedOut,
    8: BrokerNotAvailable,
    9: ReplicaNotAvailable,
    10: MessageSizeTooLarge,
    11: StaleControllerEpochCode,
    12: OffsetMetadataTooLargeCode,
    14: OffsetsLoadInProgressCode,
    15: ConsumerCoordinatorNotAvailableCode,
    16: NotCoordinatorForConsumerCode,
}


def check_and_raise_error(response):
    error = error_codes.get(response.error, Unknown)
    if error is not NoError:
        raise error(response)
