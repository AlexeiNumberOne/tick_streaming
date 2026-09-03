# from sqlalchemy import select, MetaData
# from dwh.models.models_pg import make_pairs


# def select_pairs(
#     sync_pg_engine, exchange: str, type_market: str, metadata_factory=MetaData
# ) -> list:
#     metadata_obj_pg = metadata_factory()
#     pairs = make_pairs(metadata_obj_pg)
#     query = (
#         select(pairs.columns["symbol_exchange_websocket"])
#         .where(pairs.columns["exchange"] == exchange)
#         .where(pairs.columns["type_market"] == type_market)
#     )
#     with sync_pg_engine.connect() as conn:
#         result = conn.execute(query)
#         return result.scalars().all()

from sqlalchemy import Table, select
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.engine import Connection


def select_filtered_values(
    conn: Connection | AsyncConnection,
    table: Table,
    select_columns: list,
    distinct=False,
    order_by=None,
    limit=None,
    **filters,
):
    query = select(table.columns[*select_columns]).filter_by(**filters)
    if distinct:
        query = query.distinct()
    if order_by is not None:
        query = query.order_by(order_by)

    if limit is not None:
        query = query.limit(limit)
    return conn.execute(query).scalars().all()
