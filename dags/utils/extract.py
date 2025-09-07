from datetime import datetime, timedelta
import airbyte as ab


def get_one_week_ago():
    """Returns the date one week ago in "DD-MM-YYYY" format."""
    now = datetime.now()
    return (now - timedelta(days=7)).date().strftime("%d-%m-%Y")


def run_extraction():
    """
    Extracts data from the CoinGecko API for a specific time range
    and loads it into a local DuckDB file.
    """
    try:
        # Configure the CoinGecko source
        source = ab.get_source(
            "source-coingecko-coins",
            install_if_missing=True,
            config={
                "coin_id": "bitcoin",
                "vs_currency": "usd",
                "days": "7",
                "start_date": get_one_week_ago(),
            },
            streams=["market_chart"],
        )

        source.check()

        # Configure a custom local cache with specific settings
        cache = ab.new_local_cache(
            cache_name="crypto_data",
            cache_dir="/opt/airflow/db",
            cleanup=False,
        )
        source.read(cache=cache)
    except Exception as e:
        print(f"Error during extraction: {e}")
        raise


if __name__ == "__main__":
    run_extraction()
