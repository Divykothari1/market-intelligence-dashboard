import os
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from loguru import logger
from dotenv import load_dotenv

# ----------------------------
# LOAD ENV
# ----------------------------
load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
if not NEWS_API_KEY:
    raise ValueError("❌ NEWS_API_KEY not found. Check your .env file")

# ----------------------------
# PATHS
# ----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
NEWS_OUTPUT_DIR = PROJECT_ROOT / "data/processed/news"
NEWS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# COMPANY NAME MAP (IMPORTANT)
# ----------------------------
COMPANY_NAME_MAP = {
    "TCS": "Tata Consultancy Services",
    "INFY": "Infosys",
    "RELIANCE": "Reliance Industries",
    "HDFCBANK": "HDFC Bank",
    "ICICIBANK": "ICICI Bank",
    "SBIN": "State Bank of India",
    "ITC": "ITC Limited",
    "LT": "Larsen & Toubro",
    "AXISBANK": "Axis Bank",
    "MARUTI": "Maruti Suzuki",
}

# ----------------------------
# STOCK LIST
# ----------------------------
from src.config.symbols import NIFTY_50_SYMBOLS

# ----------------------------
# NEWS API CONFIG
# ----------------------------
BASE_URL = "https://newsapi.org/v2/everything"
DAYS_LOOKBACK = 7
PAGE_SIZE = 20


def fetch_news_for_stock(symbol: str):
    """
    Fetch last 7 days news for a stock.
    Always creates a parquet file (even if no news).
    """

    stock_code = symbol.replace(".NS", "")
    query = COMPANY_NAME_MAP.get(stock_code, stock_code)

    logger.info(f"Fetching news for {stock_code} | Query: {query}")

    from_date = (datetime.utcnow() - timedelta(days=DAYS_LOOKBACK)).strftime("%Y-%m-%d")

    params = {
        "q": query,
        "from": from_date,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": PAGE_SIZE,
        "apiKey": NEWS_API_KEY
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        logger.error(f"{stock_code} failed: {response.text}")
        articles = []
    else:
        articles = response.json().get("articles", [])

    # ----------------------------
    # ALWAYS CREATE FILE
    # ----------------------------
    rows = []

    if not articles:
        logger.warning(f"No news found for {stock_code}")
        rows.append({
            "date": pd.Timestamp.utcnow(),
            "stock": stock_code,
            "headline": "No significant news in the last 7 days",
            "source": "N/A",
            "url": "",
        })
    else:
        for article in articles:
            rows.append({
                "date": article["publishedAt"],
                "stock": stock_code,
                "headline": article["title"],
                "source": article["source"]["name"],
                "url": article["url"],
            })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])

    output_file = NEWS_OUTPUT_DIR / f"{stock_code}.parquet"
    df.to_parquet(output_file, index=False)

    logger.success(f"Saved news → {output_file.name} ({len(df)} rows)")


def run_news_pipeline():
    logger.info("📰 Starting news fetch pipeline")

    for symbol in NIFTY_50_SYMBOLS:
        try:
            fetch_news_for_stock(symbol)
        except Exception as e:
            logger.error(f"{symbol} crashed: {e}")

    logger.success("📰 News fetching completed")


if __name__ == "__main__":
    run_news_pipeline()
