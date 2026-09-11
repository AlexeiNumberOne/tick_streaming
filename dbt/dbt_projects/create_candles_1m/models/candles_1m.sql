{{
    config(
        materialized='incremental',
        engine='ReplacingMergeTree()',
        order_by='(source, market_type, symbol, start_candle)',
        unique_key='(source, market_type, symbol, start_candle)'
    )
}}

WITH trades AS (
    SELECT
        DISTINCT source
        ,market_type
        ,symbol
        ,toStartOfMinute(`timestamp`) AS start_candle
        ,price
        ,volume
        ,`timestamp`
        ,toInt128OrZero(trade_id) AS trade_id
        ,CASE
            WHEN side = 'buy' THEN 1
            WHEN side = 'sell' THEN 0
            ELSE NULL
        END AS status_operation
    FROM {{ source('raw', 'raw_ticks') }}
    WHERE
        source IS NOT NULL
        AND market_type IS NOT NULL
        AND symbol IS NOT NULL
        AND price > 0
        AND volume > 0
        {% if is_incremental() %}
            AND `timestamp` >= (SELECT max(start_candle) - INTERVAL 5 MINUTE FROM {{ this }})
        {% endif %}
        AND trade_id IS NOT NULL
        AND side IN ('buy', 'sell')
)

SELECT
    source
    ,market_type
    ,symbol
    ,start_candle
    ,argMin(price, `timestamp`) AS open_price
    ,max(price) AS high_price
    ,min(price) AS low_price
    ,argMax(price, `timestamp`) AS close_price
    ,sum(volume) AS sum_volume
    ,sum(case when status_operation = 1 then volume else 0 end) as sum_volume_buy
    ,toInt64(sum_volume_buy/sum_volume * 100) as percent_volume_buy
    ,sum_volume - sum_volume_buy as sum_volume_sell
    ,100 - percent_volume_buy as percent_volume_sell
    ,count(status_operation) AS count_all_operations
    ,sum(case when status_operation = 1 then 1 else 0 end) as count_buy_operations
    ,toInt64(count_buy_operations/count_all_operations * 100 )as percent_buy_operations
    ,count_all_operations -  count_buy_operations as count_sell_operations
    ,100 - percent_buy_operations as percent_sell_operations
    ,min(trade_id) AS min_trade_id
    ,max(trade_id) AS max_trade_id
    ,(max_trade_id - min_trade_id + 1) AS expected_trades
    ,expected_trades - count_all_operations AS lost
FROM trades
GROUP BY source, market_type, symbol, start_candle
