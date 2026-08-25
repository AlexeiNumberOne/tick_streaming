import logging

from typing import Callable
from sqlalchemy import Table, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine


TableFactory = Callable[..., Table]

logger = logging.getLogger(__name__)


class DBMSManager:
    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData()
        self.models: dict[str, Table] = {}

    def register_models(self, *args: TableFactory, **kwargs) -> None:
        """
        Регистрирует таблицы, созданные фабриками.
        args - список функций, каждая принимает metadata и возвращает Table.
        """
        for factory in args:
            table = factory(self.metadata, **kwargs)
            self.models[table.name] = table

    def execute_sync(self, func: Callable, *args, **kwargs):
        if isinstance(self.engine, Engine):
            with self.engine.connect() as conn:
                return func(conn, *args, **kwargs)
        else:
            raise TypeError(
                f"Экземпляр класса имеет не верный тип атрибута engine: {type(self.engine)}"
            )

    async def execute_async(self, func: Callable, *args, **kwargs):
        if isinstance(self.engine, AsyncEngine):
            async with self.engine.connect() as conn:
                return await conn.run_sync(func, *args, **kwargs)
        else:
            raise TypeError(
                f"Экземпляр класса имеет не верный тип атрибута engine: {type(self.engine)}"
            )
