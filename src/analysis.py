"""
Module for performing text analysis and signal generation.
"""
import re
import pandas as pd

# Define simple lexicons for sentiment analysis
BULLISH_WORDS = [
    'buy', 'bullish', 'long', 'rally', 'breakout', 'uptrend', 'strong', 
    'growth', 'profit', 'up', 'rocket', 'moon', 'diamond hands'
]
BEARISH_WORDS = [
    'sell', 'bearish', 'short', 'crash', 'downtrend', 'weak', 'loss', 
    'down', 'dump', 'bubble', 'selloff', 'correction'
]

def clean_text(text: str) -> str:
    """Cleans and normalizes tweet content."""
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    text = re.sub(r'\@\w+', '', text)
    text = re.sub(r'#', '', text)
    text = re.sub(r'[^a-z\s]', '', text) # Keep only letters and spaces
    return text

def calculate_sentiment_score(text: str) -> int:
    """Calculates a simple sentiment score based on keyword matching."""
    score = 0
    words = text.split()
    for word in words:
        if word in BULLISH_WORDS:
            score += 1
        elif word in BEARISH_WORDS:
            score -= 1
    return score

def calculate_engagement_score(df: pd.DataFrame) -> pd.Series:
    """Calculates a normalized engagement score."""
    # Simple engagement: likes + (retweets * 2) to give more weight to retweets
    engagement = df['likes'] + (df['retweets'] * 2) + df['quote_count']
    
    # Normalize the score to be between 0 and 1
    if engagement.max() > engagement.min():
        normalized_engagement = (engagement - engagement.min()) / (engagement.max() - engagement.min())
    else:
        normalized_engagement = 0.0 # Avoid division by zero if all values are the same
        
    return normalized_engagement

def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates trading signals from cleaned tweet data.

    Args:
        df (pd.DataFrame): DataFrame with tweet data.

    Returns:
        pd.DataFrame: DataFrame with added signal columns.
    """
    df['cleaned_content'] = df['content'].apply(clean_text)
    df['sentiment_score'] = df['cleaned_content'].apply(calculate_sentiment_score)
    df['engagement_score'] = calculate_engagement_score(df)

    # Define weights for the composite signal
    sentiment_weight = 0.6
    engagement_weight = 0.4
    
    # Calculate composite signal
    df['composite_signal'] = (
        (df['sentiment_score'] * sentiment_weight) + 
        (df['engagement_score'] * engagement_weight)
    )
    
    return df
