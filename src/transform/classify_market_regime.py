import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger

FEATURE_DATA_DIR = Path("data/processed/features")
OUTPUT_DATA_DIR = Path("data/processed/market_regime")

OUTPUT_DATA_DIR.mkdir(parents=True, exist_ok=True)


def classify_regime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classify market regime based on trend structure.
    """

    conditions = [
        # Bullish regime
        (df["Close"] > df["sma_50"]) &
        (df["sma_10"] > df["sma_20"]) &
        (df["sma_20"] > df["sma_50"]),

        # Bearish regime
        (df["Close"] < df["sma_50"]) &
        (df["sma_10"] < df["sma_20"]) &
        (df["sma_20"] < df["sma_50"])
    ]

    choices = [
        "Bullish",
        "Bearish"
    ]

    df["market_regime"] = np.select(
        conditions,
        choices,
        default="Sideways"
    )

    return df


def process_file(file_path: Path):
    logger.info(f"Classifying market regime for {file_path.name}")

    df = pd.read_parquet(file_path)

    df = df.sort_values("Date").reset_index(drop=True)

    df = classify_regime(df)

    output_file = OUTPUT_DATA_DIR / file_path.name
    df.to_parquet(output_file, index=False)

    logger.success(f"Saved market regime data to {output_file.name}")


def run_market_regime_classification():
    parquet_files = list(FEATURE_DATA_DIR.glob("*.parquet"))

    if not parquet_files:
        logger.error("No feature parquet files found")
        return

    for file_path in parquet_files:
        process_file(file_path)


if __name__ == "__main__":
    run_market_regime_classification()
