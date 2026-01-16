import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger
from nltk.sentiment import SentimentIntensityAnalyzer

# ----------------------------
# PATHS
# ----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
NEWS_DIR = PROJECT_ROOT / "data/processed/news"

# ----------------------------
# INIT SENTIMENT MODEL
# ----------------------------
sia = SentimentIntensityAnalyzer()

def analyze_sentiment(text: str):
    scores = sia.polarity_scores(text)
    compound = scores["compound"]

    if compound >= 0.05:
        sentiment = "Positive"
    elif compound <= -0.05:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"

    confidence = abs(compound) * 100

    return sentiment, round(confidence, 2)


def process_news_file(file_path: Path):
    logger.info(f"Analyzing sentiment → {file_path.name}")

    df = pd.read_parquet(file_path)

    if df.empty:
        logger.warning("Empty file, skipping")
        return

    sentiments = []
    confidences = []

    for headline in df["headline"]:
        sentiment, confidence = analyze_sentiment(headline)
        sentiments.append(sentiment)
        confidences.append(confidence)

    df["sentiment"] = sentiments
    df["confidence"] = confidences

    df.to_parquet(file_path, index=False)
    logger.success(f"Updated sentiment → {file_path.name}")


def run_sentiment_pipeline():
    files = list(NEWS_DIR.glob("*.parquet"))

    if not files:
        logger.warning("No news files found")
        return

    for file in files:
        process_news_file(file)

    logger.success("🧠 News sentiment analysis completed")


if __name__ == "__main__":
    run_sentiment_pipeline()
