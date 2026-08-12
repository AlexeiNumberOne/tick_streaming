import os
import redis
import redis.asyncio as aioredis
import asyncio
import logging

from pathlib import Path
from redis.commands.core import AsyncScript
from redis.client import Redis as syncredis
from redis.asyncio.client import Redis as asyncredis

# from streaming.producers.producer_crypto.state import ExchangeInfo

logger = logging.getLogger(__name__)


def init_sync_redis_client() -> syncredis:
    """Инициализация синхронного Redis-клиента"""
    host = os.environ.get("REDIS_HOST", "redis")
    port = int(os.environ.get("REDIS_PORT", 6379))
    user = os.environ.get("REDIS_USER", "default")
    password = os.environ.get("REDIS_PASS")

    r = redis.Redis(host=host, port=port, username=user, password=password)

    return r


def init_async_redis_client() -> asyncredis:
    """Инициализация асинхронного Redis-клиента"""
    host = os.environ.get("REDIS_HOST", "redis")
    port = int(os.environ.get("REDIS_PORT", 6379))
    user = os.environ.get("REDIS_USER", "default")
    password = os.environ.get("REDIS_PASS")

    client = aioredis.Redis(
        host=host,
        port=port,
        username=user,
        password=password,
        decode_responses=True,
        protocol=3,  # Протокол RESP3(чтобы lua скрипты bool возвращали)
    )

    return client


def get_registered_lua_script(
    script_path: Path, redis_client: syncredis | asyncredis
) -> AsyncScript:
    """Возвращает зарегестрированный в redis lua скрипт"""
    with open(script_path, "r", encoding="utf-8") as f:
        registgred_script = redis_client.register_script(f.read())
    return registgred_script


def delete_connections(
    active_connections: dict, redis_client: syncredis | asyncredis
) -> None:
    """Удаляет активные коннекты"""
    for values in active_connections.values():
        redis_client.decrby(
            name=f"{values.exchange}:connections", amount=len(values.ws)
        )


def check_planned(redis_client: syncredis | asyncredis) -> bool:
    return int(redis_client.get("planned_SIGTERM"))


class RateLimiter:
    def __init__(
        self,
        main_key: str,
        attempt_script: AsyncScript,
        connection_script: AsyncScript,
        limit_attempt: int,
        limit_connections: int,
        ttl_attempt: int,
    ):
        self.main_key = main_key
        self.attempt_script = attempt_script
        self.connection_script = connection_script
        self.limit_attempt = limit_attempt
        self.limit_connections = limit_connections
        self.ttl_attempt = ttl_attempt

    async def access(self, timeout=300) -> bool:
        """Lua запросы к Redis для подтверждения доступности биржи, чтобы не превысить лимиты"""
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            add_attempt = await self.attempt_script(
                keys=[f"{self.main_key}:attempt"],
                args=[self.limit_attempt, self.ttl_attempt],
            )
            if add_attempt is True:
                add_connection = await self.connection_script(
                    keys=[f"{self.main_key}:connections"],
                    args=[self.limit_connections],
                )
                if not isinstance(add_connection, bool):
                    raise ValueError(
                        f"Неожиданный ответ от lua скрипта! \n {self.connection_script.script}: \n {add_connection}"
                    )
                return add_connection

            elif isinstance(add_attempt, int):
                if asyncio.get_event_loop().time() >= deadline:
                    raise TimeoutError(
                        f"[{self.main_key}] Таймаут ожидания слота для соединения. {timeout} секунд"
                    )

                logging.warning(
                    f"[{self.main_key}] "
                    f"Превышено кол-во попыток установить соединение за последние {self.ttl_attempt} секунд!\n"
                    f"Пауза перед попыткой повторно установить соединение: {add_attempt + 3} секунд"
                )
                await asyncio.sleep(add_attempt + 3)

            else:
                raise ValueError(
                    f"Неожиданный ответ от lua скрипта! \n {self.attempt_script.script}: \n {add_attempt}"
                )
