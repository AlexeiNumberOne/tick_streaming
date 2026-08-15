from sqlalchemy import Table, Column, Integer, String, TIMESTAMP, MetaData


def pairs(metadata_obj_pg=None):
    if not metadata_obj_pg:
        metadata_obj_pg = MetaData()
    return Table(
        "pairs",
        metadata_obj_pg,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("ccxt_symbol", String),
        Column("exchange", String),
        Column("symbol_exchange_rest", String),
        Column("symbol_exchange_websocket", String),
        Column("type_market", String),
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
        Column("pair", String),
        Column("event_type", String),
        Column("event_time", TIMESTAMP(timezone=True)),
        schema="crypto",
    )
