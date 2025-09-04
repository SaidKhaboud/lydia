-- Daily OHLC candlestick data derived from CoinGecko market chart data
WITH price_data AS (
  SELECT 
    prices,
    json_array_length(prices) as num_prices
  FROM {{ source('raw_crypto_data', 'market_chart') }}
),

-- Extract individual price points from the JSON array
price_points AS (
  SELECT 
    CAST(json_extract(prices, CONCAT('$[', i, '][0]')) AS BIGINT) AS timestamp_ms,
    CAST(json_extract(prices, CONCAT('$[', i, '][1]')) AS DOUBLE) AS price
  FROM price_data,
  GENERATE_SERIES(0, CAST(num_prices - 1 AS BIGINT)) AS t(i)
),

-- Convert timestamps to dates and calculate daily aggregates
daily_prices AS (
  SELECT 
    DATE_TRUNC('day', TO_TIMESTAMP(timestamp_ms / 1000.0)) AS date,
    price,
    timestamp_ms,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', TO_TIMESTAMP(timestamp_ms / 1000.0)) ORDER BY timestamp_ms ASC) AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', TO_TIMESTAMP(timestamp_ms / 1000.0)) ORDER BY timestamp_ms DESC) AS rn_desc
  FROM price_points
)

-- Calculate OHLC for each day
SELECT 
  date,
  MAX(CASE WHEN rn_asc = 1 THEN price END) AS open_price,
  MAX(price) AS high_price,
  MIN(price) AS low_price,
  MAX(CASE WHEN rn_desc = 1 THEN price END) AS close_price
FROM daily_prices
GROUP BY date
ORDER BY date