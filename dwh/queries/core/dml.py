import logging

from sqlalchemy import Connection, insert
from sqlalchemy.ext.asyncio import AsyncConnection

logger = logging.getLogger(__name__)


def insert_in_table(conn: Connection | AsyncConnection, **kwargs) -> None:
    stmt = insert(kwargs["table"]).values(kwargs["data"])
    conn.execute(stmt)
    conn.commit()
