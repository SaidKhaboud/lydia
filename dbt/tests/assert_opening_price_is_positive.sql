SELECT
    day,
    opening_price
FROM {{ ref('candlestick_data') }}
WHERE opening_price < 0
