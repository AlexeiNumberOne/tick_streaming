import logging
import asyncio
import websockets
import json
import orjson

from redis.commands.core import AsyncScript
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from aiokafka import AIOKafkaProducer
from typing import Iterator

from streaming.producers.producer_crypto.state import QUEUE_SIZE, STATS_INTERVAL

logger = logging.getLogger(__name__)


class MarketStream(ABC):
    def __init__(
        self,
        source_name: str,
        market_type: str,
        topic: str,
        producer: AIOKafkaProducer,
        add_attempt_script: AsyncScript,
        add_connection_script: AsyncScript,
        ws_url,
        limit_connections,
        limit_attempt,
        ttl_attempt,
        ready_websockets,
        ping_interval=20,
    ):
        self.source_name = source_name
        self.market_type = market_type
        self.topic = topic
        self.producer = producer
        self.add_attempt_script = add_attempt_script
        self.add_connection_script = add_connection_script
        self.ws_url = ws_url
        self.limit_connections = limit_connections
        self.limit_attempt = limit_attempt
        self.ttl_attempt = ttl_attempt
        self.ready_websockets = ready_websockets
        self.ping_interval = ping_interval

        self.queue = asyncio.Queue(maxsize=QUEUE_SIZE)
        self.stats_init_websockets = {
            "active": 0,
            "rejected": 0,
            "timeout": 0,
            "expected": 0,
        }
        self.log_count_deliveries = 0
        self.log_count_in_buffer = 0
        self.log_count_errors = 0

    @abstractmethod
    def process_message(self, raw: dict) -> Iterator[dict]:
        """Обрабатывает полученные сообщения и нормализует"""
        pass

    @abstractmethod
    def build_sub_messages() -> list:
        """Возвращает список сообщений для подписки"""
        pass

    async def _keepalive(self, ws):
        """Метод для кастомного поддержания соединения.
        Требуется не для всех бирж, поэтому не abstract
        """
        pass

    def create_websocket(self):
        """Создаёт ws соединение"""

        return websockets.connect(self.ws_url, ping_interval=self.ping_interval)

    async def _request_to_redis(self, timeout=300) -> bool:
        """Lua запросы к Redis для подтверждения доступности биржи, чтобы не превысить лимиты"""

        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            add_attempt = await self.add_attempt_script(
                keys=[f"{self.source_name}:attempt"],
                args=[self.limit_attempt, self.ttl_attempt],
            )
            if add_attempt is True:
                add_connection = await self.add_connection_script(
                    keys=[f"{self.source_name}:connections"],
                    args=[self.limit_connections],
                )
                if not isinstance(add_connection, bool):
                    raise ValueError(
                        f"Неожиданный ответ от lua скрипта! \n {self.add_connection_script.script}: \n {add_connection}"
                    )

                return add_connection

            elif isinstance(add_attempt, int):
                if asyncio.get_event_loop().time() >= deadline:
                    raise TimeoutError(
                        f"[{self.source_name} - {self.market_type}] Таймаут ожидания слота для соединения. {timeout} секунд"
                    )

                logging.warning(
                    f"[{self.source_name} - {self.market_type}] "
                    f"Превышено кол-во попыток установить соединение за последние {self.ttl_attempt} секунд!\n"
                    f"Пауза перед попыткой повторно установить соединение: {add_attempt + 3} секунд"
                )
                await asyncio.sleep(add_attempt + 3)

            else:
                raise ValueError(
                    f"Неожиданный ответ от lua скрипта! \n {self.add_attempt_script.script}: \n {add_attempt}"
                )

    async def _check_access(self, index: int = None):
        """Проверка готовности всех соединений биржи."""
        try:
            result = await self._request_to_redis(timeout=self.ttl_attempt * 5)
        except TimeoutError:
            self.stats_init_websockets["expected"] -= 1
            self.stats_init_websockets["timeout"] += 1
            logging.warning(
                f"[{self.source_name} - {self.market_type}] Соединение {index} не дождалось слота (таймаут)."
                f"Активных: {self.stats_init_websockets['active']}\n"
                f"Отклонено: {self.stats_init_websockets['rejected']}\n"
                f"В очереди: {self.stats_init_websockets['expected']}"
            )
            return False

        if not isinstance(result, bool):
            raise ValueError(
                f"Пришёл неверный тип данных от _request_to_redis: {type(result)}. Ожидается bool"
            )

        elif result is False:
            self.stats_init_websockets["expected"] -= 1
            self.stats_init_websockets["rejected"] += 1
            logging.warning(
                f"[{self.source_name} - {self.market_type}] Соединение {index} отклонено (лимит подключений).\n"
                f"Активных: {self.stats_init_websockets['active']}\n"
                f"Отклонено: {self.stats_init_websockets['rejected']}\n"
                f"В очереди: {self.stats_init_websockets['expected']}"
            )
            return False

        self.stats_init_websockets["expected"] -= 1
        self.stats_init_websockets["active"] += 1
        logging.info(
            f"[{self.source_name} - {self.market_type}] Соединение {index} получило слот.\n"
            f"Активных: {self.stats_init_websockets['active']}\n"
            f"Отклонено: {self.stats_init_websockets['rejected']}\n"
            f"В очереди: {self.stats_init_websockets['expected']}"
        )

        if self.stats_init_websockets["expected"] == 0:
            self.ready_websockets[f"{self.source_name}-{self.market_type}"] = True
            logging.info("=" * 50)
            logging.info(
                f"[{self.source_name} - {self.market_type}] установлены все соединения!"
            )
            logging.info("=" * 50)

        return True

    async def _handle_ws_message(self, raw):
        for msg in self.process_message(raw):
            await self.queue.put(msg)

    async def _read_from_ws(self, ws):
        async for message in ws:
            # Фильтр для бирж, где pong — это простая текстовая строка (OKX)
            if isinstance(message, str) and message.strip() == "pong":
                continue
            raw = orjson.loads(message)
            # Для bybit(мб удалить)
            # if self.pong_filter:
            #     if raw.get(self.pong_filter) == "ping":
            #         print(f"[bybit][{index}] Получен прикладной ping от сервера: {raw}")
            #         await ws.send(json.dumps(self.ping_msg))
            #         continue
            #     if raw.get(self.pong_filter) == 'pong':
            #         print(f"[bybit][{index}] Получен прикладной pong: {raw}")
            #         continue
            await self._handle_ws_message(raw)

    async def _create_connection(self, index: int = None, sub_msg=None):
        """Создаёт websocket соединение"""
        if not await self._check_access(index):
            return

        while True:
            try:
                async with self.create_websocket() as ws:
                    # Для OKX
                    asyncio.create_task(self._keepalive(ws))

                    await ws.send(json.dumps(sub_msg))
                    await ws.recv()
                    await self._read_from_ws(ws)

            except websockets.ConnectionClosed as e:
                logging.warning(
                    f"[{self.source_name} - {self.market_type}] Соединение закрыто ({e}))"
                )
                await self._request_to_redis()

            except Exception as e:
                logging.error(
                    f"[{self.source_name} - {self.market_type}] \nsub_msg:{sub_msg} \nКритическая ошибка: {e}"
                )
                return None

    async def _reader(self):
        """Запускает таски с websocket connections"""

        message_batches = self.build_sub_messages()
        self.stats_init_websockets["expected"] = len(message_batches)

        tasks = []
        for index, sub_msg in enumerate(message_batches):
            tasks.append(self._create_connection(index, sub_msg))
        if tasks:
            await asyncio.gather(*tasks)

    def _log_stats(self):
        logger.info(
            "=" * 20 + "\n"
            f"[{self.source_name} - {self.market_type}]\n"
            f"{(datetime.now() - timedelta(seconds=STATS_INTERVAL)).strftime("%D %H:%M:%S")} - {(datetime.now()).strftime("%D %H:%M:%S")}\n"
            f"В очереди: {self.queue.qsize()}\n"
            f"Отправлено в буфер: {self.log_count_deliveries}\n"
            f"Подтверждено: {self.log_count_in_buffer}\n"
            f"Кол-во ошибок при буферизации: {self.log_count_errors}\n"
            "=" * 20
        )
        self.log_count_deliveries = 0
        self.log_count_in_buffer = 0
        self.log_count_errors = 0

    async def _writer(self):
        last_log_time = asyncio.get_event_loop().time()

        def _on_send_done(task):
            try:
                task.result()
                self.log_count_in_buffer += 1
            except Exception as e:
                print(f"Ошибка при буферизации: {e}")
                self.log_count_errors += 1

        while True:
            try:
                msg = await self.queue.get()

                task = asyncio.create_task(self.producer.send(self.topic, value=msg))
                task.add_done_callback(_on_send_done)
                self.log_count_deliveries += 1

                now = asyncio.get_event_loop().time()
                if now - last_log_time >= STATS_INTERVAL:
                    self._log_stats()
                    last_log_time = now
            except Exception as e:
                print(
                    f"[{self.source_name} - {self.market_type}] Ошибка в _writer: {e}",
                    flush=True,
                )

    async def run(self):
        """Метод для запуска подписки websocket соединений бирж и записи в kafka тиков"""
        self.ready_websockets[f"{self.source_name}-{self.market_type}"] = False

        await asyncio.gather(self._reader(), self._writer())
