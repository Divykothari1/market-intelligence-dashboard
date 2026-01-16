import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger

PRICE_DATA_DIR = Path("data/processed/prices")
FEATURE_DATA_DIR = Path("data/processed/features")

FEATURE_DATA_DIR.mkdir(parents=True, exist_ok=True)


def build_features(file_path: Path):
    logger.info(f"Building features for {file_path.name}")

    # Read clean price data
    df = pd.read_parquet(file_path)

    # Ensure correct order
    df = df.sort_values("Date").reset_index(drop=True)

    # ----------------------------
    # FEATURE 1: DAILY RETURN
    # ----------------------------
    df["daily_return"] = df["Close"].pct_change()
    
    # ----------------------------
    # FEATURE 2: LOG RETURN
    # ----------------------------
    df["log_return"] = np.log(df["Close"] / df["Close"].shift(1))

    # ----------------------------
    # FEATURE 3: SIMPLE MOVING AVERAGES
    # ----------------------------
    df["sma_10"] = df["Close"].rolling(window=10).mean()
    df["sma_20"] = df["Close"].rolling(window=20).mean()
    df["sma_50"] = df["Close"].rolling(window=50).mean()


    # ----------------------------
    # FEATURE 4: ROLLING VOLATILITY
    # ----------------------------
    df["volatility_20"] = df["log_return"].rolling(window=20).std()



    # Save features
    output_file = FEATURE_DATA_DIR / file_path.name
    df.to_parquet(output_file, index=False)

    logger.success(f"Saved features to {output_file.name}")


def run_feature_engineering():
    parquet_files = list(PRICE_DATA_DIR.glob("*.parquet"))

    if not parquet_files:
        logger.error("No price parquet files found")
        return

    for file_path in parquet_files:
        build_features(file_path)


if __name__ == "__main__":
    run_feature_engineering()
