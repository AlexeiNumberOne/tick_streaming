import os
import redis.asyncio as aioredis

from pathlib import Path
from redis.commands.core import AsyncScript
from redis.asyncio.client import Redis


def init_redis_client() -> Redis:
    """Инициализация Redis-клиента"""
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
    script_path: Path, redis_client: Redis = None
) -> AsyncScript:
    """Возвращает зарегестрированный в redis lua скрипт"""

    redis_client = redis_client or init_redis_client()

    if not script_path.exists():
        raise FileNotFoundError(f"Файл скрипта не найден: {script_path}")

    with open(script_path, "r", encoding="utf-8") as f:
        registgred_script = redis_client.register_script(f.read())

    return registgred_script
