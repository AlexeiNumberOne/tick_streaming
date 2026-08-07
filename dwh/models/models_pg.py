from sqlalchemy import Table, Column, Integer, String


def make_pairs(metadata_obj_pg):
    return Table(
        "pairs",
        metadata_obj_pg,
        Column("id", Integer, primary_key=True),
        Column("ccxt_symbol", String),
        Column("exchange", String),
        Column("symbol_exchange_rest", String),
        Column("symbol_exchange_websocket", String),
        Column("type_market", String),
        Column("base", String),
        Column("quote", String),
        schema="crypto",
    )
