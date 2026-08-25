from sqlalchemy import MetaData, Table, Column, text
from clickhouse_sqlalchemy import types, engines


def raw_ticks(metadata_obj_ch: MetaData) -> Table:
    return Table(
        "raw_ticks",
        metadata_obj_ch,
        Column("source", types.String),
        Column("market_type", types.String),
        Column("symbol", types.String),
        Column("price", types.Decimal(22, 8)),
        Column("volume", types.Decimal(22, 8)),
        Column("timestamp", types.DateTime64),
        Column("trade_id", types.String),
        Column("side", types.String),
        engines.MergeTree(
            order_by=("source", "market_type", "symbol", "timestamp"),
            partition_by=text("toDate(timestamp)"),
            ttl=text("toDateTime(timestamp) + INTERVAL 2 DAY DELETE"),
        ),
    )


def candle_table(metadata_obj_ch: MetaData, interval: int) -> Table:
    return Table(
        f"candles_{interval}",
        metadata_obj_ch,
        Column("source", types.LowCardinality(types.String)),
        Column("market_type", types.LowCardinality(types.String)),
        Column("symbol", types.LowCardinality(types.String)),
        Column("open_time", types.DateTime),
        Column(
            "open_price",
            types.AggregateFunction(
                "argMin", types.Decimal(22, 8), types.DateTime64(3)
            ),
        ),
        Column(
            "close_price",
            types.AggregateFunction(
                "argMax", types.Decimal(22, 8), types.DateTime64(3)
            ),
        ),
        Column("high_price", types.AggregateFunction("max", types.Decimal(22, 8))),
        Column("low_price", types.AggregateFunction("min", types.Decimal(22, 8))),
        Column("volume", types.AggregateFunction("sum", types.Decimal(22, 8))),
        engines.AggregatingMergeTree(
            order_by=("source", "market_type", "symbol", "open_time"),
            partition_by=text("toDate(open_time)"),
        ),
    )
