# Message Brokers TuMeKe

Universal asyncio message broker, including Kafka and RabbitMQ (optional)

## Installation

```sh
pip install mbtumeke@git+https://github.com/tumeke-tech/mb-tumeke.git
```

## Usage
```python
from mbtumeke import Kafka, Rabbitmq

Events = {
    "test_event": {
        "event_processor": someEventProcessor,
        "serializer": lambda x: {"id":x[0], "uid": x[1], "ext":x[2]},
    },
}

MessageBrokerClass = None
if (APP_MESSAGE_BROKER == "kafka"):
    MessageBrokerClass = Kafka
elif (APP_MESSAGE_BROKER == "rabbitmq"):
    MessageBrokerClass = Rabbitmq
    
class MessageBroker(MessageBrokerClass):
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(MessageBroker, cls).__new__(cls)
            if (APP_MESSAGE_BROKER == "kafka"):
                bootstrapString = 
                bootstrap_servers = bootstrapString.split(',')
                cls.instance.set_url(bootstrap_servers)
            elif (APP_MESSAGE_BROKER == "rabbitmq"):
                cls.instance.set_url("amqp://guest:guest@127.0.0.1:5672/")
            cls.instance.set_topic_prefix("default_")
            cls.instance.set_events(Events)

        return cls.instance

```

## Dependencies

```sh
aiokafka==0.7.0
aio-pika==9.3.1
```
