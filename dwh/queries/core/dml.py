import logging

from sqlalchemy import insert, Engine, Table

logger = logging.getLogger(__name__)


def insert_info_pairs(table: Table, data: list, sync_pg_engine: Engine) -> None:
    with sync_pg_engine.connect() as conn:
        stmt = insert(table).values(data)
        conn.execute(stmt)
        conn.commit()
