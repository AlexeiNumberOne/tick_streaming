import logging

from collections.abc import Iterator
from aiokafka import AIOKafkaProducer

from streaming.producers.producer_crypto.base import MarketStream
from streaming.plugins.redis_utils import RedisManager
from dwh.postgres_utils import PostgresManager

logger = logging.getLogger(__name__)


class Binance(MarketStream):
    SUB_MSG = {"method": "SUBSCRIBE", "params": [], "id": 1}
    LIMIT_SUB = 200
    LIMIT_CONNECTION = 1024
    LIMIT_ATTEMPT = 300
    TTL_ATTEMPT = 300
    UNSUB_MSG = {"method": "UNSUBSCRIBE", "params": [], "id": 9999}
    QUEUE_SIZE = 10_000

    def __init__(
        self,
        source_name: str,
        market_type: str,
        pairs: list,
        producer: AIOKafkaProducer,
        redis_manager: RedisManager,
        async_pg_manager: PostgresManager,
    ):
        if market_type == "spot":
            ws_url = "wss://stream.binance.com:9443/ws"
        elif market_type == "swap":
            ws_url = "wss://fstream.binance.com/ws"
        elif market_type == "future":
            ws_url = "wss://dstream.binance.com/ws"
        else:
            raise ValueError(f"Неизвестный market_type: {market_type}")

        super().__init__(
            source_name=source_name,
            market_type=market_type,
            pairs=pairs,
            producer=producer,
            redis_manager=redis_manager,
            async_pg_manager=async_pg_manager,
            ws_url=ws_url,
            limit_connections=self.LIMIT_CONNECTION,
            limit_attempt=self.LIMIT_ATTEMPT,
            ttl_attempt=self.TTL_ATTEMPT,
            queue_size=self.QUEUE_SIZE,
        )

    def create_batches_pairs(self, pairs: list) -> list[list]:
        batches = [
            pairs[i : i + self.LIMIT_SUB] for i in range(0, len(pairs), self.LIMIT_SUB)
        ]
        return batches

    def build_sub_messages(self, batch: list) -> dict:
        """Возвращает сообщение для подписки"""
        msg = self.SUB_MSG.copy()
        msg["params"] = [item.lower() + "@trade" for item in batch]

        return msg

    def process_message(self, raw: dict) -> Iterator[dict]:
        try:
            if not isinstance(raw["m"], bool):
                raise ValueError
            yield {
                "source": self.source_name,
                "market_type": self.market_type,
                "symbol": str(raw["s"]),
                "price": float(raw["p"]),
                "volume": float(raw["q"]),
                "timestamp": int(raw["T"]),
                "trade_id": str(raw["t"]),
                "side": "sell" if raw["m"] else "buy",
            }
        except KeyError as e:
            logger.error(
                f"[{self.source_name} - {self.market_type}] Ошибка парсинга сообщения: отсутствует поле {e}."
            )
            return ()
        except ValueError as e:
            logger.error(
                f"[{self.source_name} - {self.market_type}] Ошибка парсинга сообщения: неверный тип данных {e}."
            )
            return ()
