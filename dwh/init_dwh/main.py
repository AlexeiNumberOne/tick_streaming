import ccxt
import logging

from sqlalchemy import MetaData, Engine
from config.crypto.loader_sources import load_config_sources
from dwh.engines.engine_pg import get_sync_pg_engine
from dwh.models.models_pg import make_pairs
from dwh.queries.core.ddl import create_schemas_pg, create_tables_pg
from dwh.queries.core.dml import insert_info_pairs


logger = logging.getLogger(__name__)


def ddl_postgres(
    sync_pg_engine: Engine,
    metadata_factory=MetaData,
    make_pairs_func=make_pairs,
    create_schemas=create_schemas_pg,
    create_tables=create_tables_pg,
):
    """Создаёт схемы и таблицы в Postgres"""
    create_schemas(sync_pg_engine=sync_pg_engine)
    metadata_obj_pg = metadata_factory()
    make_pairs_func(metadata_obj_pg=metadata_obj_pg)
    create_tables(sync_pg_engine=sync_pg_engine, metadata_obj_pg=metadata_obj_pg)


def rest_get_pairs(exchange: str, needed_types: list):
    try:
        exchange_class = getattr(ccxt, exchange)
        exchange_instance = exchange_class()
        markets = exchange_instance.load_markets()
    except AttributeError:
        logging.error(f"Биржа {exchange} не найдена в ccxt!")
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


def dml_postgres(
    sync_pg_engine: Engine,
    config_loader=load_config_sources,
    metadata_factory=MetaData,
    make_pairs_func=make_pairs,
    rest_get_pairs_func=rest_get_pairs,
    insert_pairs_func=insert_info_pairs,
):
    """Загружает данные о парах с бирж и вставляет в Postgres"""
    exchanges = config_loader()
    metadata_obj_pg = metadata_factory()
    table = make_pairs_func(metadata_obj_pg)

    for exchange in exchanges:
        data = rest_get_pairs_func(exchange["name_exchange"], exchange["type_markets"])
        if data:
            insert_pairs_func(table=table, data=data, sync_pg_engine=sync_pg_engine)
        else:
            logging.warning(f"Для {exchange} не были получены данные")
            continue


def main(
    pg_engine_factory=get_sync_pg_engine, ddl_pg=ddl_postgres, dml_pg=dml_postgres
):
    """
    Главная функция с фабриками движков и функциями для DDL/DML.
    """
    sync_pg_engine = pg_engine_factory()
    ddl_pg(sync_pg_engine)
    dml_pg(sync_pg_engine)


if __name__ == "__main__":
    main()
