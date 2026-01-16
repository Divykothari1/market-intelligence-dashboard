import yfinance as yf
import pandas as pd
from pathlib import Path
from loguru import logger
from src.config.symbols import NIFTY_50_SYMBOLS


# Where raw price data will be stored
RAW_PRICE_DIR = Path("data/raw/prices")
RAW_PRICE_DIR.mkdir(parents=True, exist_ok=True)

# Stocks we track (NIFTY large caps)


START_DATE = "2018-01-01"

def fetch_stock_data(symbol: str):
    logger.info(f"Fetching data for {symbol}")

    df = yf.download(
        symbol,
        start=START_DATE,
        interval="1d",
        progress=False
    )

    if df.empty:
        logger.warning(f"No data received for {symbol}")
        return

    df.reset_index(inplace=True)
    df["symbol"] = symbol

    file_path = RAW_PRICE_DIR / f"{symbol}.csv"
    df.to_csv(file_path, index=False)

    logger.success(f"Saved data for {symbol}")

if __name__ == "__main__":
    for symbol in NIFTY_50_SYMBOLS:
        fetch_stock_data(symbol)
