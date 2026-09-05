from streaming.producers.producer_crypto.ws import WSManager


class ExchangeInfo:
    """Информация о готовности биржи
    и её ws-соединениях
    """

    def __init__(self, exchange: str, market_type: str, expected_ws: int):
        self.exchange = exchange
        self.market_type = market_type
        self.expected_ws = expected_ws
        self.ws: list[WSManager] = []
        self.log_count_deliveries = 0
        self.log_count_in_buffer = 0
        self.log_count_errors = 0
        self.log_count_received_ticks = 0

    def add_manager(self, manager: WSManager):
        self.ws.append(manager)

    @property
    def is_ready(self) -> bool:
        if not self.ws:
            return False
        count = 0
        for ws in self.ws:
            if ws.info.health is False:
                return False
            count += 1

        if count != self.expected_ws:
            return False

        return True


exchange_state: dict[str, ExchangeInfo] = {}
