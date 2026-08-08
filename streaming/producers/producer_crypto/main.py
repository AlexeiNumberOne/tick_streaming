import asyncio
import logging

from pathlib import Path

from streaming.producers.producer_crypto import exchanges
from streaming.producers.producer_crypto.state import READY_EXCHANGES, READY_WEBSOCKETS
from streaming.producers.producer_crypto.api import run_server
from streaming.plugins import kafka_utils
from dwh.engines.engine_pg import get_sync_pg_engine
from streaming.plugins.redis_client import init_redis_client, get_registered_lua_script
from config.crypto.loader_sources import load_config_sources
from dwh.queries.core.dql import select_pairs

logger = logging.getLogger(__name__)


async def main():
    server_task = asyncio.create_task(run_server())

    try:
        ready_exchanges = load_config_sources(valid_exchanges=READY_EXCHANGES)

        if not isinstance(ready_exchanges, list):
            raise TypeError(
                f"Неверный тип для exchanges: {type(ready_exchanges)}. Ожидается list"
            )
        if not ready_exchanges:
            raise ValueError("Пришёл пустой список exchanges")

        STREAM_CLASSES = {
            name: getattr(exchanges, name.capitalize()) for name in READY_EXCHANGES
        }

        bootstrap_servers = kafka_utils.get_bootstrap_servers()
        sync_pg_engine = get_sync_pg_engine()
        redis_client = init_redis_client()

        add_connection_script = get_registered_lua_script(
            redis_client=redis_client,
            script_path=Path("streaming/plugins/lua/add_active_connection.lua"),
        )
        add_attempt_script = get_registered_lua_script(
            redis_client=redis_client,
            script_path=Path("streaming/plugins/lua/add_attempt_create_connect.lua"),
        )

        await kafka_utils.wait_kafka(bootstrap_servers)

        producer = kafka_utils.create_producer(bootstrap_servers)

        streams = []

        for exchange in ready_exchanges:
            name_exchange = exchange["name_exchange"]

            if not await kafka_utils.exists_topic(
                topic_name=name_exchange, bootstrap_servers=bootstrap_servers
            ):
                await kafka_utils.create_topic(name_exchange, bootstrap_servers)

            for type_market in exchange["type_markets"]:
                pairs = select_pairs(sync_pg_engine, name_exchange, type_market)
                stream = STREAM_CLASSES[name_exchange](
                    source_name=name_exchange,
                    market_type=type_market,
                    pairs=pairs,
                    topic=name_exchange,
                    producer=producer,
                    add_attempt_script=add_attempt_script,
                    add_connection_script=add_connection_script,
                    ready_websockets=READY_WEBSOCKETS,
                )

                streams.append(stream)

        await producer.start()
        await asyncio.gather(*(stream.run() for stream in streams))

    except Exception:
        logger.exception("Ошибка в main")
        server_task.cancel()
        raise


if __name__ == "__main__":
    asyncio.run(main())
