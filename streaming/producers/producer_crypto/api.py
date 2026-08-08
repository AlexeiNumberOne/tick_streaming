import uvicorn

from fastapi import FastAPI

from streaming.producers.producer_crypto.state import READY_WEBSOCKETS

app = FastAPI()


def is_ready() -> bool:
    """Возвращает True, если все websocket подключения были установлены"""

    if not isinstance(READY_WEBSOCKETS, dict):
        raise ValueError(
            f"Неверный тип данных для READY_WEBSOCKETS: {type(READY_WEBSOCKETS)}: {READY_WEBSOCKETS}. Ожидается dict"
        )

    return bool(READY_WEBSOCKETS) and all(READY_WEBSOCKETS.values())


@app.get("/ready")
async def ready():
    """Ручка для получения статуса готовности"""

    if is_ready():
        return {"status": "ok"}
    return {"status": "not ready"}, 503


async def run_server():
    config = uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()
