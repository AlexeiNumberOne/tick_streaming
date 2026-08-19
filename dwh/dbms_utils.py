import logging

from typing import Callable
from sqlalchemy import Table, MetaData
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import create_engine, Engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

TableFactory = Callable[[MetaData], Table]

logger = logging.getLogger(__name__)


class PostgresSettings(BaseSettings):
    HOST: str
    PORT: int
    USER: str
    PASS: str
    NAME: str

    @property
    def sync_url(self) -> str:
        return f"postgresql+psycopg://{self.USER}:{self.PASS}@{self.HOST}:{self.PORT}/{self.NAME}"

    @property
    def async_url(self) -> str:
        return f"postgresql+asyncpg://{self.USER}:{self.PASS}@{self.HOST}:{self.PORT}/{self.NAME}"

    model_config = SettingsConfigDict(env_file=".env", env_prefix="PG_", extra="ignore")


class ClickHouseSettings(BaseSettings):
    HOST: str
    PORT: str
    USER: str
    PASS: str
    NAME: str

    @property
    def sync_url(self) -> str:
        return f"clickhouse+native://{self.USER}:{self.PASS}@{self.HOST}:{self.PORT}/{self.NAME}"

    @property
    def async_url(self) -> str:
        return f"clickhouse+asynch://{self.USER}:{self.PASS}@{self.HOST}:{self.PORT}/{self.NAME}"

    model_config = SettingsConfigDict(env_file=".env", env_prefix="CH_", extra="ignore")


class DBMSManager:
    def __init__(
        self, settings: PostgresSettings | ClickHouseSettings, use_async: bool = False
    ):
        self.settings = settings

        self._sync_engine: Engine = None
        self._async_engine: AsyncEngine = None

        self.metadata = MetaData()
        self.models: dict[str, Table] = {}

        if use_async:
            if self._async_engine is None:
                self._async_engine = create_async_engine(self.settings.async_url)
        else:
            if self._sync_engine is None:
                self._sync_engine = create_engine(self.settings.sync_url)

    def register_models(self, *args: TableFactory) -> None:
        """
        Регистрирует таблицы, созданные фабриками.
        args - список функций, каждая принимает metadata и возвращает Table.
        """
        for factory in args:
            table = factory(self.metadata)
            self.models[table.name] = table

    def execute_sync(self, func, *args, **kwargs):
        if self._sync_engine is None:
            raise ValueError(f"Экземпляр класса {DBMSManager.__name__} был создан в асинхронном режиме\
                                \nДля асинхронного режима - method: execute_async, либо при создании экземпляра use_async=False")
        with self._sync_engine.connect() as conn:
            return func(conn, *args, **kwargs)

    async def execute_async(self, func, *args, **kwargs):
        if self._async_engine is None:
            raise ValueError(f"Экземпляр класса {DBMSManager.__name__} был создан в синхронном режиме\
                             \nДля синхронного режима - method: execute_sync, либо при создании экземпляра use_async=True")
        async with self._async_engine.connect() as conn:
            return await conn.run_sync(func, *args, **kwargs)
