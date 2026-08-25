import ccxt
import logging

from dwh.postgres_utils import PGManager
from dwh.clickhouse_utils import CHManager
from dwh.models.models_pg import pairs, event_log
from dwh.models.models_ch import candle_table, raw_ticks
from dwh.queries.core.ddl import create_schemas, create_tables
from dwh.queries.core.dml import insert_in_table
from dwh.queries.raw.ddl import create_mvw_ch

from config.crypto.loader_sources import load_config_sources
from config.config import INTERVAL_MAP

logger = logging.getLogger(__name__)


def rest_get_pairs(exchange: str, needed_types: list) -> list[dict]:
    try:
        exchange_class = getattr(ccxt, exchange)
        exchange_instance = exchange_class()
        markets = exchange_instance.load_markets()
    except AttributeError:
        logger.error(f"Биржа {exchange} не найдена в ccxt!")
        return
    except Exception as e:
        logger.error(f"Ошибка при загрузке рынков для {exchange}: {e}")
        return

    data = []

    for symbol, info in markets.items():
        if info["type"] not in needed_types:
            continue

        data.append(
            {
                "ccxt_symbol": symbol,
                "exchange": exchange,
                "symbol_exchange_rest": info["id"],
                "symbol_exchange_websocket": info["symbol"]
                if exchange == "kraken"
                else info["id"],
                "type_market": info["type"],
                "base": info["base"],
                "quote": info["quote"],
            }
        )

    return data


def main():
    pg_manager = PGManager()
    pg_manager.register_models(pairs, event_log)
    pg_manager.execute_sync(create_schemas, "crypto")
    pg_manager.execute_sync(create_tables, pg_manager.metadata)

    exchanges = load_config_sources()

    for exchange in exchanges:
        data = rest_get_pairs(exchange["name_exchange"], exchange["type_markets"])
        if data:
            pg_manager.execute_sync(
                insert_in_table, table=pg_manager.models["pairs"], values=data
            )
        else:
            logging.warning(f"Для {exchange} не были получены данные")
            continue

    ch_manager = CHManager()
    ch_manager.register_models(raw_ticks)
    for interval in INTERVAL_MAP:
        ch_manager.register_models(candle_table, interval=interval)
    ch_manager.execute_sync(create_tables, ch_manager.metadata)
    ch_manager.execute_sync(create_mvw_ch, intervals=INTERVAL_MAP)


if __name__ == "__main__":
    main()
