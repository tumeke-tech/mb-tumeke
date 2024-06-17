from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener, TopicPartition
from datetime import datetime
from json import loads, dumps
from .executor import Executor
import asyncio
import logging
import random
import string
import traceback

def serializer(x):
    return dumps(x).encode('utf-8')

def deserializer(x):
    return loads(x.decode('utf-8'))

def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def build_offset(msg):
    tp = TopicPartition(msg.topic, msg.partition)
    ret = {}
    ret[tp] = msg.offset + 1
    return ret

class MyRebalancer(ConsumerRebalanceListener):

    async def on_partitions_revoked(self, revoked):
        print("Partition revoked!!")

    async def on_partitions_assigned(self, assigned):
        print("Partition assigned!!")
    
class Kafka():
    def set_url(self, url):
        self.url = url

    def set_topic_prefix(self, topic_prefix = ""):
        self.topic_prefix = topic_prefix

    def set_events(self, events):
        self.events = events

    def kconsumer(self,
                  group_id,
                  loop=None,
                  max_poll_interval_ms=300000):
        return AIOKafkaConsumer(
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=group_id,
            loop=loop,
            max_poll_records=1,
            max_poll_interval_ms=max_poll_interval_ms,
            value_deserializer=deserializer,
            bootstrap_servers=self.url,
            session_timeout_ms=60000,
            heartbeat_interval_ms=10000,
            isolation_level="read_committed")

    def kproducer(self,
                  transactional_id=None,
                  loop=None,
                  transaction_timeout_ms=60000):
        return AIOKafkaProducer(
            value_serializer=serializer,
            loop=loop,
            transaction_timeout_ms=transaction_timeout_ms,
            transactional_id=transactional_id,
            bootstrap_servers=self.url,
            enable_idempotence=True
        )

    async def start(self, loop, topics, is_video=False):
        self.topics = [self.topic_prefix+t for t in topics]
        self.group_id = self.topic_prefix+'compute'
        self.loop = loop

        print('Kafka Loaded')
        print(f'Watching topics {topics}')

        self.done_processing = asyncio.Event()
        
        # max_processing_time is used to set both of the following for long-running processing
        # max.poll.interval.ms = maximum delay between invocations of poll() for consumer
        # transaction.timeout.ms = max time broker will wait for a transaction initiated by producer
        max_processing_time = 1000*60*60 # 1hr
        consumer = self.kconsumer(self.group_id,
            loop=self.loop,
            max_poll_interval_ms = max_processing_time)
        consumer.subscribe(topics, listener=MyRebalancer())
        await consumer.start()

        for partition in consumer.assignment():
            print("Partition: " + str(partition), flush=True)

        producer = self.kproducer('compute_' + get_random_string(8),
            loop=self.loop,
            transaction_timeout_ms = max_processing_time)
        await producer.start()
        executor = Executor()

        try:
            # process loop
            while True:
                # watches all topics, get an event from a random one
                # consume single message with a transaction, proccesses, commits
                print("Entered while loop", flush=True)
                msg = await consumer.getone()
                time = datetime.fromtimestamp(msg.timestamp/1000).strftime('%Y-%m-%d %H:%M:%SZ')
                print(f"Pulled event: {msg.partition}, {msg.offset}, {time}", flush=True)
                async with producer.transaction():
                    
                    # info about event
                    topic = msg.topic
                    event = msg.value
                    print(f'consumed {topic} {event}', flush=True)
                    # process
                    processed_topic = topic[len(self.topic_prefix):]
                    eventProcessor = self.events[processed_topic]["event_processor"]
                    print(".5", flush=True)
                    executor.run(eventProcessor, msg)
                    await self.done_processing.wait()
                    self.done_processing.clear()
                    out_topic, out_val, with_prefix = executor.result()
                    
                    if out_topic:
                        if (with_prefix):
                            out_topic = self.topic_prefix+out_topic
                        await producer.send(out_topic, out_val)
                        print(f'published {out_topic} {out_val}', flush=True)
                    commit_offset = build_offset(msg)
                    await producer.send_offsets_to_transaction(
                        commit_offset, self.group_id)
                    print("Sent offset to transaction", flush=True)
        except Exception as e:
            logging.error(traceback.format_exc())
        finally:
            # leave consumer group
            await consumer.stop()
            await producer.stop()
    
    async def push(self, topic, company, tup):
        processed_topic = topic
        if (company is not None and company.kafka_prefix is not None):
            processed_topic = company.kafka_prefix + topic

        event = self.events[topic]["serializer"](tup)
        producer = self.kproducer()
        await producer.start()
        try:
            await producer.send_and_wait(processed_topic, event)
            print(f'published {processed_topic} {event}')
        except Exception as e:
            logging.error(traceback.format_exc())
        finally:
            await producer.stop()

    def create_message(self, topic, content, withPrefix = False):
        # packs content according to topic serializer
        if type(content) is tuple:
            event = self.events[topic]["serializer"](content)
        else:
            event = content
        return topic, event, withPrefix

    def put_fail(self, msg, error_msg="", error_traceback=""):
        event = msg.value
        event["orig_topic"] = event["orig_topic"] if "orig_topic" in event else msg.topic
        event["error_msg"] = error_msg
        event["error_traceback"] = error_traceback
        publish_topic = "failed"
        return self.create_message(publish_topic, event, True)

    def done(self):
       self.done_processing.set()
