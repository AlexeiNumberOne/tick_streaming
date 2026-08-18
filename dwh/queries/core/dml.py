import logging

from sqlalchemy import Connection, insert, Table
from sqlalchemy.ext.asyncio import AsyncConnection

logger = logging.getLogger(__name__)


def insert_in_table(
    conn: Connection | AsyncConnection, table=Table, values=list
) -> None:
    stmt = insert(table).values(values)
    conn.execute(stmt)
    conn.commit()
