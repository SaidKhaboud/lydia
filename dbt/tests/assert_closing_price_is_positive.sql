SELECT
    day, closing_price
FROM {{ ref('candlestick_data') }}
WHERE closing_price < 0
