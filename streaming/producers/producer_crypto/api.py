import uvicorn

from fastapi import FastAPI

from streaming.producers.producer_crypto.state import exchange_state

app = FastAPI()


def is_ready() -> bool:
    """Возвращает True, если все websocket подключения были установлены"""

    for values in exchange_state.values():
        if not values.is_ready:
            return False

    return True


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
