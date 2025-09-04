from datetime import datetime, timedelta
import airbyte as ab

def get_one_week_ago():
    now = datetime.now()
    return (now - timedelta(days=7)).date()

def run_extraction():
    """
    Extracts data from the CoinGecko API for a specific time range
    and loads it into a local DuckDB file.
    """

    # Configure the CoinGecko source
    source = ab.get_source(
        "source-coingecko-coins",
        install_if_missing=True,
        config={
            "coin_id": "bitcoin",
            "vs_currency": "usd",
            "days": "7",
            "start_date": get_one_week_ago().strftime("%d-%m-%Y"),
        },
        streams=["market_chart"],
    )

    source.check()
    
    # Configure a custom local cache with specific settings
    cache = ab.new_local_cache(
        cache_name="crypto_data",  # Custom cache name
        cache_dir="./",  # Store in current directory
        cleanup=False  # Don't auto-cleanup the cache
    )
    source.read(cache=cache)

if __name__ == "__main__":
    run_extraction()