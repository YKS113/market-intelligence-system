"""
Main script for the market intelligence pipeline.
"""
import os
from src.scraper import run_concurrent_scraper
from src.processor import process_and_store_data

# --- Configuration ---
HASHTAGS = ['nifty50', 'sensex', 'banknifty', 'intraday', 'stockmarketindia']
TOTAL_TWEET_LIMIT = 2000 # The overall target number of tweets
DATA_DIR = "data"
RAW_DATA_PATH = os.path.join(DATA_DIR, "raw_tweets.csv")
PROCESSED_DATA_PATH = os.path.join(DATA_DIR, "signals.parquet")

def main():
    """Main function to run the data pipeline."""
    # Ensure data directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    # --- 1. Data Collection ---
    print("--- Starting Data Collection Stage ---")
    # Calculate limit per hashtag to reach the total limit
    limit_per_hashtag = TOTAL_TWEET_LIMIT // len(HASHTAGS)
    limit_per_hashtag = 10 # X API v2 free tier constraint
    raw_df = run_concurrent_scraper(HASHTAGS, limit_per_hashtag)

    if not raw_df.empty:
        # Save raw data to CSV
        raw_df.to_csv(RAW_DATA_PATH, index=False)
        print(f"Raw data saved to {RAW_DATA_PATH}")

        # --- 2. Data Processing ---
        print("\n--- Starting Data Processing Stage ---")
        process_and_store_data(
            input_path=RAW_DATA_PATH,
            output_path=PROCESSED_DATA_PATH
        )
    else:
        print("No data was scraped. Pipeline finished.")
        
    print("\n--- Pipeline execution complete. ---")

if __name__ == "__main__":
    main()
