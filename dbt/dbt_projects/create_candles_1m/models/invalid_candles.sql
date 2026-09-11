{{
    config(
        materialized='view',
        post_hook=[
            "INSERT INTO {{ source('raw', 'invalid_candles_bridge') }} (source, market_type, symbol, start_candle)
             SELECT * FROM {{ this }}
            "
        ]
    )
}}

SELECT
    DISTINCT source,
    market_type,
    symbol,
    start_candle
FROM {{ ref('candles_1m') }}
WHERE
    lost > 0
    AND start_candle >= (
            SELECT max(start_candle) - INTERVAL 5 MINUTE
            FROM {{ ref('candles_1m') }}
        )
