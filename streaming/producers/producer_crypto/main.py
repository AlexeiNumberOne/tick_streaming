import asyncio
import logging
import signal
import time

from pathlib import Path

from streaming.producers.producer_crypto import exchanges
from streaming.producers.producer_crypto.state import exchange_state
from streaming.producers.producer_crypto.exchanges import SUPPORTED_EXCHANGES
from streaming.producers.producer_crypto.api import run_server
from streaming.plugins.kafka_utils import KafkaManager

from streaming.plugins.redis_utils import RedisManager

from datetime import datetime, timezone

from config.crypto.loader_sources import load_config_sources

from dwh.dbms_utils import DBMSManager, PostgresSettings
from dwh.queries.core.dql import select_filtered_values
from dwh.queries.core.dml import insert_in_table
from dwh.models.models_pg import pairs, event_log

logger = logging.getLogger(__name__)

settings_pg = PostgresSettings()
sync_pg_manager = DBMSManager(settings_pg)
sync_pg_manager.register_models(pairs, event_log)


def shutdown(signum, frame):
    shutdown_time = datetime.fromtimestamp(int(time.time()), tz=timezone.utc)
    logging.warning(f"Принят сигнал {signum} в  {frame}")
    redis_manager = RedisManager(use_async=False)

    data = []
    event_type = "planned_shutdown"
    if not redis_manager.planned_SIGTERM:
        logger.warning(f"Незапланированный сигнал {signum}")
        event_type = "emergency_shutdown"

    for exchange in exchange_state.values():
        for ws in exchange.ws:
            for pair in ws.info.pairs:
                data.append(
                    {
                        "exchange": exchange.exchange,
                        "pair": pair,
                        "event_type": event_type,
                        "event_time": shutdown_time,
                    }
                )
            # 🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️🖍️
            # Отсылать сообщение об отписке

    sync_pg_manager.execute_sync(
        insert_in_table, table=sync_pg_manager.models["event_log"], values=data
    )

    redis_manager.delete_connections(exchange_state)


signal.signal(signal.SIGTERM, shutdown)


async def main():
    server_task = asyncio.create_task(run_server())

    try:
        ready_exchanges = load_config_sources(valid_exchanges=SUPPORTED_EXCHANGES)

        STREAM_CLASSES = {
            name: getattr(exchanges, name.capitalize()) for name in SUPPORTED_EXCHANGES
        }

        redis_manager = RedisManager(use_async=True)
        redis_manager.register_lua_script(
            Path("streaming/plugins/lua/attempt_script.lua"),
            Path("streaming/plugins/lua/connection_script.lua"),
        )

        kafka_manager = KafkaManager()
        kafka_manager.create_producer()

        async_pg_manager = DBMSManager(settings_pg, use_async=True)
        async_pg_manager.register_models(event_log)

        streams = []
        required_topics = []
        for exchange in ready_exchanges:
            name_exchange = exchange["name_exchange"]
            required_topics.append(name_exchange)

            for type_market in exchange["type_markets"]:
                pairs = sync_pg_manager.execute_sync(
                    select_filtered_values,
                    table=sync_pg_manager.models["pairs"],
                    select_columns=["symbol_exchange_websocket"],
                    exchange=name_exchange,
                    type_market=type_market,
                )
                stream = STREAM_CLASSES[name_exchange](
                    source_name=name_exchange,
                    market_type=type_market,
                    pairs=pairs,
                    producer=kafka_manager.producer,
                    redis_manager=redis_manager,
                    async_pg_manager=async_pg_manager,
                )

                streams.append(stream)

        await kafka_manager.exists_topics(required_topics)
        await kafka_manager.producer.start()
        await asyncio.gather(*(stream.run() for stream in streams))

    except Exception:
        logger.exception("Ошибка в main")
        server_task.cancel()
        raise


if __name__ == "__main__":
    asyncio.run(main())
