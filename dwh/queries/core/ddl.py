import logging

from sqlalchemy import MetaData, Engine
from sqlalchemy.schema import CreateSchema

logger = logging.getLogger(__name__)


def create_schemas_pg(sync_pg_engine: Engine) -> None:
    try:
        with sync_pg_engine.connect() as conn:
            conn.execute(CreateSchema("crypto", if_not_exists=True))
            conn.commit()

        logger.info("Схемы успешно созданы")
    except Exception as e:
        logger.error(f"Ошибка при создании схем в postgres: {e}")


def create_tables_pg(sync_pg_engine: Engine, metadata_obj_pg: MetaData) -> None:
    try:
        metadata_obj_pg.create_all(sync_pg_engine)
        logger.info("Таблицы в postgres успешно созданы")
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц в postgres: {e}")
