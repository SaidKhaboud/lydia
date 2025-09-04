import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle

def generate_visualization():
    """
    Connects to DuckDB, fetches the transformed candlestick data,
    and generates a candlestick chart showing OHLC data.
    """
    print("Generating candlestick visualization...")
    try:
        # Use the same database path as specified in the dbt profiles.yml
        db_path = '/Users/saidkhaboud/Desktop/lydia/db_crypto_data.duckdb'
        con = duckdb.connect(database=db_path, read_only=True)
        df = con.execute("SELECT date, open_price, high_price, low_price, close_price FROM daily_candlestick ORDER BY date").fetchdf()
        con.close()

        if df.empty:
            print("No data found to visualize.")
            return

        # Convert date column to datetime if it isn't already
        df['date'] = pd.to_datetime(df['date'])
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Create candlestick chart
        for i, row in df.iterrows():
            date = row['date']
            open_price = row['open_price']
            high_price = row['high_price']
            low_price = row['low_price']
            close_price = row['close_price']
            
            # Determine color: green if close > open, red if close < open
            color = 'green' if close_price >= open_price else 'red'
            
            # Draw the high-low line
            ax.plot([date, date], [low_price, high_price], color='black', linewidth=1)
            
            # Draw the open-close rectangle (candlestick body)
            body_height = abs(close_price - open_price)
            body_bottom = min(open_price, close_price)
            
            # Create rectangle for the candlestick body
            rect = Rectangle((date - pd.Timedelta(hours=8), body_bottom), 
                           pd.Timedelta(hours=16), body_height,
                           facecolor=color, edgecolor='black', alpha=0.8)
            ax.add_patch(rect)
            
            # Draw small lines for open and close if body is very small
            if body_height < (high_price - low_price) * 0.01:  # Very small body
                ax.plot([date - pd.Timedelta(hours=6), date], [open_price, open_price], color='black', linewidth=1)
                ax.plot([date, date + pd.Timedelta(hours=6)], [close_price, close_price], color='black', linewidth=1)

        ax.set_title('Bitcoin Daily Candlestick Chart (OHLC)', fontsize=16, fontweight='bold')
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Price (USD)', fontsize=12)
        ax.grid(True, alpha=0.3)
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        plt.xticks(rotation=45)
        
        # Add legend
        green_patch = plt.Rectangle((0, 0), 1, 1, facecolor='green', alpha=0.8, label='Bullish (Close > Open)')
        red_patch = plt.Rectangle((0, 0), 1, 1, facecolor='red', alpha=0.8, label='Bearish (Close < Open)')
        ax.legend(handles=[green_patch, red_patch], loc='upper left')
        
        plt.tight_layout()

        output_path = 'bitcoin_candlestick_chart.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Candlestick chart saved successfully to {output_path}")

    except Exception as e:
        print(f"An error occurred during visualization: {e}")
        raise

    except Exception as e:
        print(f"An error occurred during visualization: {e}")
        raise

if __name__ == "__main__":
    generate_visualization()