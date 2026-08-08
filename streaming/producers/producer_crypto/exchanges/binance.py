import logging

from collections.abc import Iterator
from aiokafka import AIOKafkaProducer
from redis.commands.core import AsyncScript

from streaming.producers.producer_crypto.base import MarketStream

logger = logging.getLogger(__name__)

SUB_MSG = {"method": "SUBSCRIBE", "params": [], "id": 1}
LIMIT_SUB = 200
LIMIT_CONNECTION = 1024
LIMIT_ATTEMPT = 300
TTL_ATTEMPT = 300


class Binance(MarketStream):
    def __init__(
        self,
        source_name: str,
        market_type: str,
        pairs: list,
        topic: str,
        producer: AIOKafkaProducer,
        add_attempt_script: AsyncScript,
        add_connection_script: AsyncScript,
        ready_websockets: dict,
    ):
        self.pairs = pairs
        self.limit_sub = LIMIT_SUB

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
            topic=topic,
            producer=producer,
            add_attempt_script=add_attempt_script,
            add_connection_script=add_connection_script,
            ws_url=ws_url,
            limit_connections=LIMIT_CONNECTION,
            limit_attempt=LIMIT_ATTEMPT,
            ttl_attempt=TTL_ATTEMPT,
            ready_websockets=ready_websockets,
        )

    def build_sub_messages(self) -> list:
        """Возвращает список сообщений для подписки"""
        if not self.pairs:
            raise ValueError("Пришёл пустой список валютный пар")

        messages = []
        batches = [
            self.pairs[i : i + self.limit_sub]
            for i in range(0, len(self.pairs), self.limit_sub)
        ]

        for batch in batches:
            msg = SUB_MSG.copy()
            msg["params"] = [item.lower() + "@trade" for item in batch]
            messages.append(msg)

        return messages

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
            logging.error(
                f"[{self.source_name} - {self.market_type}] Ошибка парсинга сообщения: отсутствует поле {e}."
            )
            return ()
        except ValueError as e:
            logging.error(
                f"[{self.source_name} - {self.market_type}] Ошибка парсинга сообщения: неверный тип данных {e}."
            )
            return ()
