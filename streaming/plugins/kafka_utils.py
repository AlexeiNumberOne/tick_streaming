import asyncio
import time
import orjson
import os
import logging

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from streaming.producers.producer_crypto.state import ExchangeInfo


def get_bootstrap_servers(bootstrap_servers: str = None) -> str | list[str]:
    """
    Возвращает bootstrap_servers
    Если передан явно – используется он
    Иначе – берётся из переменной окружения KAFKA_BOOTSTRAP_SERVERS
    Если переменной нет – возвращается дефолт "localhost:9092"
    """

    if bootstrap_servers:
        return bootstrap_servers

    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


async def wait_kafka(
    bootstrap_servers: str | list[str],
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
            admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
            await admin.start()
            await admin.close()
            print("Kafka готова к работе!")
            return
        except Exception as e:
            print(f"Kafka недоступна ({e}), повтор через {retry_interval} секунд...")

            if time.time() - start >= timeout:
                raise TimeoutError(
                    f"Не удалось подключиться к Kafka за {timeout} секунд"
                )

            if attempts >= max_retries:
                raise RuntimeError(
                    f"Не удалось подключиться к Kafka за {max_retries} попыток"
                )

            await asyncio.sleep(retry_interval)


async def exists_topic(topic_name: str, bootstrap_servers: str | list[str]):
    """Проверка существования топика в Kafka"""

    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=bootstrap_servers, client_id="check_topic"
    )

    await admin_client.start()

    try:
        topics = await admin_client.list_topics()

        if topic_name in topics:
            print(f"Топик '{topic_name}' найден")
            return True

        print(f"Топик '{topic_name}' не был найден")
        return False

    except Exception as e:
        print(f"При проверке топика произошла ошибка {e}")
        raise
    finally:
        await admin_client.close()


async def create_topic(topic_name: str, bootstrap_servers: str | list[str]):
    """Создание топика в Kafka"""

    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=bootstrap_servers, client_id="create_topic"
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

    await admin_client.start()

    try:
        await admin_client.create_topics([topic])
        print(f"Топик '{topic_name}' успешно создан.")
        # time.sleep(5)
        return
    except TopicAlreadyExistsError:
        print(f"Топик '{topic_name}' уже был создан другим клиентом.")
    except Exception as e:
        print(f"При создании топика произошла ошибка {e}")
        raise
    finally:
        await admin_client.close()


def create_producer(bootstrap_servers: str | list[str]) -> AIOKafkaProducer:
    """
    Создание и настройка экземпляра AIOKafkaProducer
    """

    return AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: orjson.dumps(v),
        linger_ms=200,
        max_batch_size=1048576,  # 1 МБ
        compression_type="gzip",
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
                logging.error(
                    f"{self.exchange_info.exchange} Ошибка при буфферизации: {e}",
                    flush=True,
                )
                self.exchange_info.log_count_errors += 1
