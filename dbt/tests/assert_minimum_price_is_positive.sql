SELECT
    day,
    minimum_price
FROM {{ ref('candlestick_data') }}
WHERE minimum_price < 0
