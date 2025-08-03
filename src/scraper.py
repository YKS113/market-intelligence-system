"""
Module for scraping tweet data from Twitter/X using snscrape.
"""
import snscrape.modules.twitter as sntwitter
import pandas as pd
from datetime import datetime, timedelta
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import tweepy

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s', filename='app.log', filemode='a', encoding='utf-8')

# Define the data structure for a tweet
TWEET_COLUMNS = [
    'id', 'username', 'timestamp', 'content', 'likes', 
    'retweets', 'replies', 'quote_count', 'mentions', 'hashtags', 'url'
]

BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAOiP3QEAAAAAU5AQUIfLGblGCAIvjZLWYm1ojVE%3D4aHF7j2Hof5sVCtd66wYQztOB0nH9J7jrh9jBeJhqsEOhdf4T4'

def get_tweets_from_x(hashtag, limit=10, since_date=None):
    """
    Scrapes tweets for a given hashtag using the X (Twitter) API v2 and Tweepy.

    Args:
        hashtag (str): The hashtag to search for (e.g., "Python"). Do NOT include the "#" prefix here.
        limit (int): The maximum number of tweets to retrieve.
        since_date (str, optional): Start date for tweets in 'YYYY-MM-DD' format.
                                    If None, defaults to the last 7 days (standard for Free/Basic tiers).
                                    Full archive access requires higher tiers.

    Returns:
        list: A list of dictionaries, where each dictionary represents a tweet.
    """
    tweets_list = []
    print('entered twtr')
    if not BEARER_TOKEN:
        logging.error("Error: BEARER_TOKEN is not set. Please get your token from the X Developer Portal.")
        return []

    try:
        # Initialize the Tweepy client with your Bearer Token
        client = tweepy.Client(BEARER_TOKEN)
        print('client started', client)

        # Construct the query for recent search (past 7 days is typical for free/basic)
        query_string = f"#{hashtag} -is:retweet" # Exclude retweets for cleaner data

        # Add start_time if provided, for historical search (requires appropriate API access)
        start_time = None
        if since_date:
            try:
                # API expects ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)
                # We'll use the start of the day
                dt_object = datetime.strptime(since_date, '%Y-%m-%d')
                start_time = dt_object.isoformat() + "Z"
                logging.info(f"Using start_time: {start_time}")
            except ValueError:
                logging.warning(f"Invalid since_date format: {since_date}. Using default (last 7 days).")
                start_time = None # Reset to None if invalid format

        logging.info(f"Querying X for: '{query_string}' with limit={limit}")

        # Use search_recent_tweets for recent data (past 7 days)
        # For full archive, you'd use client.search_all_tweets (requires Academic or Enterprise access)
        response = client.search_recent_tweets(
            query=query_string,
            tweet_fields=["created_at", "author_id", "public_metrics", "entities"], # Request additional fields
            expansions=["author_id"], # To get user details
            max_results=min(limit,10) # Max 1 results per request for recent search
        )
        print('seach res', response)
        # Handle rate limiting or other API errors
        if response.errors:
            for error in response.errors:
                logging.error(f"X API Error: {error.get('title')} - {error.get('detail')}")
            return []

        if response.data:
            users = {user["id"]: user for user in response.includes.get('users', [])}

            for tweet in response.data:
                author_username = users.get(tweet.author_id, {}).get('username', 'N/A')
                
                # Extract hashtags and mentions from entities
                tweet_hashtags = [h['tag'] for h in tweet.entities.get('hashtags', [])] if tweet.entities else []
                tweet_mentions = [m['username'] for m in tweet.entities.get('mentions', [])] if tweet.entities else []

                tweets_list.append({
                    'id': tweet.id,
                    'username': author_username,
                    'timestamp': tweet.created_at,
                    'content': tweet.text,
                    'likes': tweet.public_metrics.get('like_count', 0),
                    'retweets': tweet.public_metrics.get('retweet_count', 0),
                    'replies': tweet.public_metrics.get('reply_count', 0),
                    'quote_count': tweet.public_metrics.get('quote_count', 0),
                    'mentions': tweet_mentions,
                    'hashtags': tweet_hashtags,
                    'url': f"https://twitter.com/{author_username}/status/{tweet.id}" # Construct URL
                })
            logging.info(f"Successfully retrieved {len(tweets_list)} tweets for #{hashtag}.")
        else:
            logging.info(f"No tweets found for #{hashtag} with the given query parameters.")

    except tweepy.TweepyException as e:
        logging.error(f"Tweepy error while scraping {hashtag}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while scraping {hashtag}: {e}")

    return tweets_list

def run_concurrent_scraper(hashtags: List[str], limit_per_hashtag: int = 10) -> pd.DataFrame:
    """
    Runs scrapers for multiple hashtags concurrently using a thread pool.

    Args:
        hashtags (List[str]): A list of hashtags to search for.
        limit_per_hashtag (int): The maximum number of tweets to fetch per hashtag.

    Returns:
        pd.DataFrame: A DataFrame containing the scraped tweets.
    """
    since_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    all_tweets = []
    
    logging.info(f"Starting concurrent scraping for {len(hashtags)} hashtags...")

    with ThreadPoolExecutor(max_workers=len(hashtags)) as executor:
        # Submit scraping tasks to the thread pool
        future_to_hashtag = {
            executor.submit(get_tweets_from_x, hashtag, limit_per_hashtag, since_date): hashtag
            for hashtag in hashtags
        }

        for future in as_completed(future_to_hashtag):
            hashtag = future_to_hashtag[future]
            try:
                result = future.result()
                all_tweets.extend(result)
                logging.info(f"Successfully scraped {len(result)} tweets for {hashtag}.")
            except Exception as e:
                logging.error(f"Scraping task for {hashtag} generated an exception: {e}")

    if not all_tweets:
        logging.warning("No tweets were scraped. Exiting.")
        return pd.DataFrame(columns=TWEET_COLUMNS)

    df = pd.DataFrame(all_tweets)
    # Ensure all columns are present, even if empty
    for col in TWEET_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[TWEET_COLUMNS] # Enforce column order
    
    logging.info(f"Total unique tweets scraped: {len(df)}")
    return df
