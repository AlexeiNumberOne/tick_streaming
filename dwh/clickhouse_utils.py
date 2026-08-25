import os

from asynch.pool import Pool
from sqlalchemy import create_engine
from typing import Callable

from dwh.dbms_utils import DBMSManager


class CHManager(DBMSManager):
    HOST: str = os.environ.get("CH_HOST")
    PORT: int = os.environ.get("CH_PORT")
    USER: str = os.environ.get("CH_USER")
    PASS: str = os.environ.get("CH_PASS")
    NAME: str = os.environ.get("CH_NAME")

    def __init__(self, use_async: bool = False):
        if use_async:
            self.engine = Pool(
                minsize=1,
                maxsize=10,
                dsn=f"clickhouse://{self.USER}:{self.PASS}@{self.HOST}:{self.PORT}/{self.NAME}",
            )
        else:
            self.engine = create_engine(
                f"clickhouse+native://{self.USER}:{self.PASS}@{self.HOST}:{self.PORT}/{self.NAME}"
            )

        super().__init__(engine=self.engine)

    async def execute_async(self, func: Callable, *args, **kwargs):
        if isinstance(self.engine, Pool):
            async with self.engine.connection() as conn:
                return await func(conn, *args, **kwargs)
        else:
            raise TypeError(
                f"Экземпляр класса имеет не верный тип атрибута engine: {type(self.engine)}"
            )
