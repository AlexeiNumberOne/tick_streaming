from sqlalchemy import Table, Column, Integer, String, TIMESTAMP, MetaData


def pairs(metadata_obj_pg=None):
    if not metadata_obj_pg:
        metadata_obj_pg = MetaData()
    return Table(
        "pairs",
        metadata_obj_pg,
        Column("ccxt_symbol", String, primary_key=True),
        Column("exchange", String, primary_key=True),
        Column("type_market", String, primary_key=True),
        Column("symbol_exchange_rest", String),
        Column("symbol_exchange_websocket", String),
        Column("base", String),
        Column("quote", String),
        schema="crypto",
    )


def event_log(metadata_obj_pg=None):
    if not metadata_obj_pg:
        metadata_obj_pg = MetaData()
    return Table(
        "event_log",
        metadata_obj_pg,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("exchange", String),
        Column("pair", String),
        Column("event_type", String),
        Column("event_time", TIMESTAMP(timezone=True)),
        schema="crypto",
    )


def invalid_candles(metadata_obj_pg):
    return Table(
        "invalid_candles",
        metadata_obj_pg,
        Column("source", String, primary_key=True),
        Column("market_type", String, primary_key=True),
        Column("symbol", String, primary_key=True),
        Column("start_candle", String, primary_key=True),
        schema="crypto",
    )
