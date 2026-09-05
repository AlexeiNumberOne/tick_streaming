import logging

from sqlalchemy import insert, Table
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


def insert_in_table(
    conn: Connection | AsyncConnection, table=Table, values=list
) -> None:
    stmt = insert(table).values(values)
    conn.execute(stmt)
    conn.commit()
