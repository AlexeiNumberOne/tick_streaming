import asyncio
import time
import orjson
import os
import logging

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import (
    TopicAlreadyExistsError,
    NoBrokersAvailable,
    KafkaConnectionError,
)

from streaming.producers.producer_crypto.state import ExchangeInfo

logger = logging.getLogger(__name__)


class KafkaManager:
    def __init__(
        self,
        topics: list = None,
        bootstrap_servers: str = None,
    ):
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.topics = topics
        self.producer = None
        self.consumer = None

    async def wait_kafka(
        self,
        timeout: int = 180,
        max_retries: int = 10,
        retry_interval: int = 15,
    ):
        """Ожидание запуска Kafka"""
        attempts = 0
        start = time.time()
        while True:
            attempts += 1
            try:
                admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
                await admin.start()
                await admin.close()
                logger.info("Kafka готов к работе!")
                return
            except Exception as e:
                logger.warning(
                    f"Kafka недоступна ({e}), повтор через {retry_interval} секунд..."
                )
                if time.time() - start >= timeout:
                    raise TimeoutError(
                        f"Не удалось подключиться к Kafka за {timeout} секунд"
                    )
                if attempts >= max_retries:
                    raise RuntimeError(
                        f"Не удалось подключиться к Kafka за {max_retries} попыток"
                    )
                await asyncio.sleep(retry_interval)

    async def exists_topics(self, topics):
        """Проверка существования топика в Kafka"""
        admin_client = AIOKafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers, client_id="check_topic"
        )
        required_topics = topics or self.topics
        try:
            await admin_client.start()
            current_topics = await admin_client.list_topics()
            for topic in required_topics:
                if topic not in current_topics:
                    logger.warning(f"Топик '{topic}' не был найден")
                    await self.create_topic(topic_name=topic)

        except (NoBrokersAvailable, KafkaConnectionError, TimeoutError):
            await self.wait_kafka()

        finally:
            await admin_client.close()

    async def create_topic(self, topic_name: str):
        """Создание топика в Kafka"""
        admin_client = AIOKafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id=f"create_topic_{topic_name}",
        )
        topic = NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=1,
            topic_configs={
                "retention.ms": "86400000",  # 24 часа
                "cleanup.policy": "delete",
                "compression.type": "gzip",
            },
        )
        try:
            await admin_client.start()
            await admin_client.create_topics([topic])
            logger.info(f"Топик '{topic_name}' успешно создан.")

        except (NoBrokersAvailable, KafkaConnectionError, TimeoutError):
            await self.wait_kafka()

        except TopicAlreadyExistsError:
            logger.warning(f"Топик '{topic_name}' уже был создан другим клиентом.")

        finally:
            await admin_client.close()

    def create_producer(self) -> AIOKafkaProducer:
        """
        Создание и настройка экземпляра AIOKafkaProducer
        """
        if self.consumer is not None:
            raise ValueError(
                "Экземпляр класса не должен быть использован как producer и consumer одновременно!"
            )

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: orjson.dumps(v),
            linger_ms=200,
            max_batch_size=1048576,  # 1 МБ
            compression_type="gzip",
        )

    def create_consumer(self) -> AIOKafkaConsumer:
        """
        Создание и настройка экземпляра AIOKafkaConsumer
        """
        if self.producer is not None:
            raise ValueError(
                "Экземпляр класса не должен быть использован как producer и consumer одновременно!"
            )

        self.consumer = AIOKafkaConsumer(
            self.topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id="clickhouse-writer",
            value_deserializer=lambda v: orjson.loads(v),
            auto_offset_reset="latest",  # earliest
            enable_auto_commit=False,
        )


class KafkaWriter:
    def __init__(
        self,
        queue: asyncio.Queue,
        producer: AIOKafkaProducer,
        exchange_info: ExchangeInfo,
    ):
        self.queue = queue
        self.producer = producer
        self.exchange_info = exchange_info

    def _on_send_done(self, task):
        try:
            task.result()
            self.exchange_info.log_count_deliveries += 1
        except Exception as e:
            print(f"Ошибка при отправке: {e}")
            self.exchange_info.log_count_errors += 1

    async def _send_message(self, msg):
        return await self.producer.send(self.exchange_info.exchange, value=msg)

    async def run(self):
        while True:
            try:
                msg = await self.queue.get()
                task = asyncio.create_task(self._send_message(msg))
                task.add_done_callback(self._on_send_done)
                self.exchange_info.log_count_in_buffer += 1
                await asyncio.sleep(0)
            except Exception as e:
                logger.error(
                    f"{self.exchange_info.exchange} Ошибка при буфферизации: {e}",
                    flush=True,
                )
                self.exchange_info.log_count_errors += 1
