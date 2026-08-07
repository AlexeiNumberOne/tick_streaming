from sqlalchemy.engine import create_engine, Engine
from dwh.settings.setting_pg import PostgresSettings


def get_sync_pg_engine() -> Engine:
    setting = PostgresSettings()
    return create_engine(url=setting.pg_url)
