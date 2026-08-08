import pytest
import redis.asyncio as aioredis

from pathlib import Path

from streaming.plugins.redis_client import get_registered_lua_script


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_lua_scripts(redis_container):
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)

    client = aioredis.Redis(host=host, port=port, decode_responses=True, protocol=3)

    add_attempt = get_registered_lua_script(
        redis_client=client,
        script_path=Path("streaming/plugins/lua/add_attempt_create_connect.lua"),
    )
    add_connection = get_registered_lua_script(
        redis_client=client,
        script_path=Path("streaming/plugins/lua/add_active_connection.lua"),
    )

    result = await add_attempt(keys=["test:attempt"], args=[1, 10])
    assert result is True

    result = await add_attempt(keys=["test:attempt"], args=[1, 10])
    assert result == 10

    result = await add_connection(keys=["test:connections"], args=[1])
    assert result is True

    result = await add_connection(keys=["test:connections"], args=[1])
    assert result is False
