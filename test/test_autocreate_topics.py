from kafkaka.gevent_patch import KafkaClient
c = KafkaClient("t-storm1:9092", topic_names=['test7'])