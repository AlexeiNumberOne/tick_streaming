from sqlalchemy import Connection, text


def create_mvw_ch(conn: Connection, intervals: dict) -> None:
    for interval, seconds in intervals.items():
        name_table = f"candles_{interval}"
        name_mvw = f"mwv_{interval}"
        open_price = "argMinState(price, timestamp)"
        close_price = "argMaxState(price, timestamp)"
        high_price = "maxState(price)"
        low_price = "minState(price)"
        volume = "sumState(volume)"

        conn.execute(
            text(f"""
            CREATE MATERIALIZED VIEW {name_mvw} TO {name_table} AS
            SELECT
                source,
                market_type,
                symbol,
                toStartOfInterval(timestamp, INTERVAL {seconds} SECOND)     AS open_time,
                {open_price}                                                AS open_price,
                {close_price}                                               AS close_price,
                {high_price}                                                AS high_price,
                {low_price}                                                 AS low_price,
                {volume}                                                    AS volume
            FROM raw_ticks
            GROUP BY source, market_type, symbol, open_time
            """)
        )

    conn.commit()
