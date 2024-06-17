from aio_pika import DeliveryMode, ExchangeType, Message, connect_robust, connect
from aio_pika.abc import AbstractIncomingMessage
from .executor import Executor
from json import loads, dumps
import asyncio
import traceback

class JsonPayload:
    def __init__(self, message, topic):
        self.value = loads(message.decode("utf-8"))
        self.value["orig_topic"] = topic
        self.topic = topic
    
class Rabbitmq():
    def set_url(self, url):
        self.url = url

    def set_topic_prefix(self, topic_prefix = ""):
        self.topic_prefix = topic_prefix

    def set_events(self, events):
        self.events = events
    
    async def start(self, loop, topics, is_video=False):
        self.topics = [self.topic_prefix+t for t in topics]
        self.group_id = self.topic_prefix+'compute'
        self.loop = loop

        print('Rabbitmq Loaded')
        print(f'Watching topics {topics}')
        
        self.done_processing = asyncio.Event()

        connection = await connect_robust(self.url, loop=loop)

        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        
        self.topics_exchange = await channel.declare_exchange(
            "topics",
            ExchangeType.TOPIC,
            durable=True,
        )

        self.queue = await channel.declare_queue(
            self.group_id,
            durable=True,
            exclusive=False,
            auto_delete=False,
        )

        self.executor = Executor()

        for topic in self.topics:
            await self.queue.bind(self.topics_exchange, routing_key=topic)
            print("Topic binded: " + str(topic), flush=True)

        print("Entered while loop", flush=True)
        await self.queue.consume(self.on_message, no_ack=False)

        await asyncio.Future()
    
    async def push(self, topic, company, tup):
        processed_topic = topic
        if (company is not None and company.kafka_prefix is not None):
            processed_topic = company.kafka_prefix + topic

        event = self.events[topic]["serializer"](tup)

        connection = await connect(self.url)

        async with connection:
            # Creating a channel
            channel = await connection.channel()

            logs_exchange = await channel.declare_exchange(
                "topics",
                ExchangeType.TOPIC,
                durable=True,
            )

            message = Message(
                dumps(event).encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
            )

            await logs_exchange.publish(message, routing_key=processed_topic)

            await connection.close()

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

    async def on_message(self, message: AbstractIncomingMessage) -> None:
        async with message.process(ignore_processed=True):
            print(f"Pulled event: {message.routing_key!r}", flush=True)

            processed_topic = message.routing_key

            processed_topic = processed_topic[len(self.topic_prefix):]
            eventProcessor = self.events[processed_topic]["event_processor"]

            out_topic = None
            out_val = None
            with_prefix = None
            try:
                payload = JsonPayload(message.body, processed_topic)
                self.executor.run(eventProcessor, payload)
                await self.done_processing.wait()
                self.done_processing.clear()
                out_topic, out_val, with_prefix = self.executor.result()
            except Exception as e:
                str1 = traceback.format_exc()
                errorMsg = str(e)
                errorTraceback = str1
                payload = JsonPayload("{}".encode(), processed_topic)
                out_topic, out_val, with_prefix = self.put_fail(payload, errorMsg, errorTraceback)

            if out_topic:
                if (with_prefix):
                    out_topic = self.topic_prefix+out_topic

                return_message = Message(
                    dumps(out_val).encode(),
                    delivery_mode=DeliveryMode.PERSISTENT,
                )
                await self.topics_exchange.publish(return_message, routing_key=out_topic)
                print(f'published {out_topic} {out_val}', flush=True)

            await message.ack()

            print("Entered while loop", flush=True)

    def done(self):
       self.done_processing.set()
