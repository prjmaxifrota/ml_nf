import time
from queue import Queue
import pika  # RabbitMQ client
import redis
from kafka import KafkaProducer, KafkaConsumer
from enum import Enum, auto

class QueueType(Enum):
    PYTHON_QUEUE = auto()
    AZURE_REDIS = auto()
    NATIVE_REDIS = auto()
    RABBIT_MQ = auto()
    KAFKA = auto()

class MessageQueue:
    def __init__(self, queue_type: QueueType, connection_params=None, queue='default', is_consumer=False):
        self.queue_type = queue_type
        self.connection_params = connection_params or {}
        self.queue_name = queue  # Queue name is set here
        self.is_consumer = is_consumer
        self.client = None
        self.channel = None
        self.connect()  # Initialize the connection

    def connect(self):
        """Establish a connection based on the queue type."""
        try:
            if self.queue_type == QueueType.PYTHON_QUEUE:
                self.client = Queue()
                print("Connected to Python Queue.")

            elif self.queue_type in {QueueType.AZURE_REDIS, QueueType.NATIVE_REDIS}:
                self.client = redis.Redis(
                    host=self.connection_params.get('host', 'localhost'),
                    port=self.connection_params.get('port', 6379),
                    username=self.connection_params.get('username'),
                    password=self.connection_params.get('password'),
                    ssl=self.queue_type == QueueType.AZURE_REDIS
                )
                print(f"Connected to {'Azure Redis' if self.queue_type == QueueType.AZURE_REDIS else 'Native Redis'}.")

            elif self.queue_type == QueueType.RABBIT_MQ:
                self._connect_rabbitmq()
                print("Connected to RabbitMQ.")

            elif self.queue_type == QueueType.KAFKA:
                self._connect_kafka()
                print("Connected to Kafka.")

            else:
                raise ValueError("Unsupported QueueType")
        except Exception as e:
            print(f"Connection error: {e}. Retrying in 5 seconds...")
            time.sleep(5)
            self.connect()  # Retry connection

    def _connect_rabbitmq(self):
        """Establish RabbitMQ connection and channel."""
        try:
            credentials = pika.PlainCredentials(
                self.connection_params.get('username', ''), 
                self.connection_params.get('password', '')
            ) if self.connection_params.get('username') else None

            connection_params = pika.ConnectionParameters(
                host=self.connection_params.get('host', 'localhost'),
                port=self.connection_params.get('port', 5672),
                credentials=credentials
            )
            
            # Establish the connection
            self.client = pika.BlockingConnection(connection_params)
            
            # Create a channel from the connection
            self.channel = self.client.channel()
            
            # Declare the queue on the channel
            self.channel.queue_declare(queue=self.queue_name)
            print(f"Connected to RabbitMQ and declared queue '{self.queue_name}'.")

        except Exception as e:
            print(f"Failed to connect to RabbitMQ: {e}. Retrying in 5 seconds...")
            time.sleep(5)
            self._connect_rabbitmq()

    def _connect_kafka(self):
        """Establish Kafka producer or consumer connection."""
        servers = self.connection_params.get('servers', f"{self.connection_params.get('host', 'localhost')}:{self.connection_params.get('port', 9092)}")
        if self.is_consumer:
            self.client = KafkaConsumer(self.queue_name, bootstrap_servers=servers, group_id=self.connection_params.get('group_id', 'default'))
        else:
            self.client = KafkaProducer(bootstrap_servers=servers)

    def publish(self, message):
        """Publish a message to the queue."""
        try:
            if self.queue_type == QueueType.PYTHON_QUEUE:
                self.client.put(message)
                print("Message published to Python Queue.")

            elif self.queue_type in {QueueType.AZURE_REDIS, QueueType.NATIVE_REDIS}:
                self.client.publish(self.queue_name, message)
                print(f"Message published to Redis channel '{self.queue_name}'.")

            elif self.queue_type == QueueType.RABBIT_MQ:
                if self.client.is_closed or self.channel.is_closed:
                    self._connect_rabbitmq()
                self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
                print(f"Message published to RabbitMQ queue '{self.queue_name}'.")

            elif self.queue_type == QueueType.KAFKA:
                self.client.send(self.queue_name, value=message.encode())
                print(f"Message published to Kafka topic '{self.queue_name}'.")

        except Exception as e:
            print(f"Publish error: {e}. Reconnecting and retrying...")
            self.connect()
            self.publish(message)

    def subscribe_and_consume(self, callback):
        """Consume messages from the queue using a callback function."""
        if not self.is_consumer:
            raise ValueError("This instance is not configured as a consumer.")
        
        while True:
            try:
                if self.queue_type == QueueType.PYTHON_QUEUE:
                    self._consume_python_queue(callback)

                elif self.queue_type in {QueueType.AZURE_REDIS, QueueType.NATIVE_REDIS}:
                    self._consume_redis(callback)

                elif self.queue_type == QueueType.RABBIT_MQ:
                    self._consume_rabbitmq(callback)

                elif self.queue_type == QueueType.KAFKA:
                    self._consume_kafka(callback)

            except Exception as e:
                print(f"Consumption error in {self.queue_type}: {e}. Retrying...")
                time.sleep(5)
                self.connect()

    def _consume_python_queue(self, callback):
        """Consume messages from Python queue."""
        while not self.client.empty():
            callback(self.client.get())

    def _consume_redis(self, callback):
        """Consume messages from Redis PubSub."""
        pubsub = self.client.pubsub()
        pubsub.subscribe(self.queue_name)
        for message in pubsub.listen():
            if message['type'] == 'message':
                callback(message['data'])

    def _consume_rabbitmq(self, callback):
        """Consume messages from RabbitMQ."""
        def rabbitmq_callback(ch, method, properties, body):
            callback(body)

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=rabbitmq_callback, auto_ack=True)
        print(f"Consuming from RabbitMQ queue '{self.queue_name}'...")
        self.channel.start_consuming()

    def _consume_kafka(self, callback):
        """Consume messages from Kafka topic."""
        for message in self.client:
            callback(message.value.decode())
