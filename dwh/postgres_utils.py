import os

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from dwh.dbms_utils import DBMSManager


class PGManager(DBMSManager):
    HOST: str = os.environ.get("PG_HOST")
    PORT: int = os.environ.get("PG_PORT")
    USER: str = os.environ.get("PG_USER")
    PASS: str = os.environ.get("PG_PASS")
    NAME: str = os.environ.get("PG_NAME")

    def __init__(self, use_async: bool = False):
        if use_async:
            self.engine = create_async_engine(
                f"postgresql+asyncpg://{self.USER}:{self.PASS}@{self.HOST}:{self.PORT}/{self.NAME}"
            )
        else:
            self.engine = create_engine(
                f"postgresql+psycopg://{self.USER}:{self.PASS}@{self.HOST}:{self.PORT}/{self.NAME}"
            )

        super().__init__(engine=self.engine)
