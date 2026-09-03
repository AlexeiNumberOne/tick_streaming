import logging

from sqlalchemy import MetaData
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.schema import CreateSchema

logger = logging.getLogger(__name__)


def create_schemas(conn: Connection | AsyncConnection, *args: str) -> None:
    try:
        for schema in args:
            conn.execute(CreateSchema(schema, if_not_exists=True))
        conn.commit()
        logger.info(f"Успешно созданы схемы:\n{'\n'.join(args)}")
    except Exception as e:
        logger.error(f"Ошибка при создании схем:\n{'\n'.join(args)}\n{e}")
        raise


def create_tables(conn: Connection | AsyncConnection, metadata: MetaData) -> None:
    try:
        table_names = [table.name for table in metadata.tables.values()]
        metadata.create_all(conn)
        conn.commit()
        logger.info(f"Таблицы:\n{'\n'.join(table_names)}\nУспешно созданы")
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц:\n{'\n'.join(table_names)}\n{e}")
        raise
