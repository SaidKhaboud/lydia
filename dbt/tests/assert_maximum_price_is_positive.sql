SELECT
    day,
    maximum_price
FROM {{ ref('candlestick_data') }}
WHERE maximum_price < 0
