import os
import redis
import redis.asyncio as aioredis
import asyncio
import logging

from pathlib import Path
from redis.commands.core import AsyncScript
from redis.client import Script

logger = logging.getLogger(__name__)


class RedisManager:
    host = os.environ.get("REDIS_HOST", "redis")
    port = int(os.environ.get("REDIS_PORT", 6379))
    user = os.environ.get("REDIS_USER", "default")
    password = os.environ.get("REDIS_PASS")

    def __init__(self, use_async: bool = False):
        self.__use_async = use_async

        connect_params = {
            "host": self.host,
            "port": self.port,
            "username": self.user,
            "password": self.password,
            "decode_responses": True,
            "protocol": 3,  # Протокол RESP3(чтобы lua скрипты bool возвращали)
        }

        if self.__use_async:
            self.r = aioredis.Redis(**connect_params)

        else:
            self.r = redis.Redis(**connect_params)

        self.lua_scripts: dict[str, Script] = {}

    def register_lua_script(self, *args: Path) -> AsyncScript:
        """Регистрирует в redis lua скрипт"""
        for path in args:
            path = Path(path)
            with open(path, "r", encoding="utf-8") as f:
                registgred_script = self.r.register_script(f.read())

            self.lua_scripts[path.name.split(".")[0]] = registgred_script

    def delete_connections(self, active_connections: dict) -> None:
        """Удаляет активные коннекты"""
        for values in active_connections.values():
            self.r.decrby(name=f"{values.exchange}:connections", amount=len(values.ws))

    @property
    def planned_SIGTERM(self) -> bool:
        value = self.r.get("planned_SIGTERM")
        if value is None:
            return False
        elif int(value) == 0:
            return True
        else:
            raise ValueError(
                f"Пришло не правильное значение ключа planned_SIGTERM: {value}"
            )


class RateLimiter:
    def __init__(
        self,
        manager: RedisManager,
        main_key: str,
        limit_attempt: int,
        limit_connections: int,
        ttl_attempt: int,
    ):
        self.manager = manager
        self.main_key = main_key
        self.limit_attempt = limit_attempt
        self.limit_connections = limit_connections
        self.ttl_attempt = ttl_attempt

    async def access(self, timeout=300) -> bool:
        """Lua запросы к Redis для подтверждения доступности биржи, чтобы не превысить лимиты"""
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            add_attempt = await self.manager.lua_scripts["attempt_script"](
                keys=[f"{self.main_key}:attempt"],
                args=[self.limit_attempt, self.ttl_attempt],
            )
            if add_attempt is True:
                add_connection = await self.manager.lua_scripts["connection_script"](
                    keys=[f"{self.main_key}:connections"],
                    args=[self.limit_connections],
                )
                if not isinstance(add_connection, bool):
                    raise ValueError(
                        f"Неожиданный ответ от lua скрипта! \n\
                        {self.manager.lua_scripts['connection_script'].script}:\
                        \n{add_connection}"
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
                    f"Неожиданный ответ от lua скрипта!\n\
                    {self.manager.lua_scripts['attempt_script'].script}:\n\
                    {add_attempt}"
                )
