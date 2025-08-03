"""
Module for processing raw scraped data.
Handles cleaning, deduplication, and storage.
"""
import pandas as pd
import logging
from typing import Optional

from src.analysis import generate_signals

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s', filename='app.log', filemode='a', encoding='utf-8')

def process_and_store_data(input_path: str, output_path: str) -> Optional[pd.DataFrame]:
    """
    Loads raw data, processes it, and stores it in Parquet format.

    Args:
        input_path (str): The path to the raw CSV file.
        output_path (str): The path to save the processed Parquet file.
        
    Returns:
        pd.DataFrame or None: The processed DataFrame, or None if input is empty.
    """
    try:
        df = pd.read_csv(input_path)
        if df.empty:
            logging.warning("Input file is empty. No processing will be done.")
            return None
    except FileNotFoundError:
        logging.error(f"Input file not found at {input_path}")
        return None

    logging.info(f"Loaded {len(df)} raw tweets.")

    # 1. Data Type Conversion and Cleaning
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    numeric_cols = ['likes', 'retweets', 'replies', 'quote_count']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # Drop rows with invalid timestamps
    df.dropna(subset=['timestamp', 'content'], inplace=True)

    # 2. Deduplication
    initial_rows = len(df)
    df.drop_duplicates(subset=['id'], keep='first', inplace=True)
    df.drop_duplicates(subset=['username', 'content'], keep='first', inplace=True)
    logging.info(f"Removed {initial_rows - len(df)} duplicate tweets.")

    # 3. Generate Signals
    df_processed = generate_signals(df)
    
    # 4. Store in Parquet format
    try:
        df_processed.to_parquet(output_path, engine='pyarrow', index=False)
        logging.info(f"Successfully processed and saved data to {output_path}")
    except Exception as e:
        logging.error(f"Failed to save data to Parquet file: {e}")
        return None
        
    return df_processed
