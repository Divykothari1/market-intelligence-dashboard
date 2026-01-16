import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger

# -------------------------
# Paths
# -------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
MARKET_REGIME_DIR = PROJECT_ROOT / "data/processed/market_regime"
SIGNAL_OUTPUT_DIR = PROJECT_ROOT / "data/processed/signals"

SIGNAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # -------------------------
    # Base signal
    # -------------------------
    df["signal_label"] = "Neutral"
    df["signal_strength"] = "Weak"

    bullish_condition = (
        (df["market_regime"] == "Bullish") &
        (df["Close"] > df["sma_20"])
    )

    bearish_condition = (
        (df["market_regime"] == "Bearish") &
        (df["Close"] < df["sma_20"])
    )

    df.loc[bullish_condition, "signal_label"] = "Bullish"
    df.loc[bullish_condition, "signal_strength"] = "Strong"

    df.loc[bearish_condition, "signal_label"] = "Bearish"
    df.loc[bearish_condition, "signal_strength"] = "Strong"

    # -------------------------
    # Confidence score
    # -------------------------
    df["confidence_score"] = 50

    df.loc[bullish_condition, "confidence_score"] += 20
    df.loc[bearish_condition, "confidence_score"] += 20

    df.loc[df["Close"] > df["sma_50"], "confidence_score"] += 15
    df.loc[df["Close"] < df["sma_50"], "confidence_score"] += 15

    df["confidence_score"] = df["confidence_score"].clip(0, 100)

    # -------------------------
    # Expected move (%)
    # -------------------------
    df["expected_move_pct"] = (
        df["volatility_20"] * np.sqrt(5) * 100
    ).round(2)

    df.loc[df["signal_label"] == "Bearish", "expected_move_pct"] *= -1

    # -------------------------
    # Risk level
    # -------------------------
    df["risk_level"] = "Medium"
    df.loc[df["confidence_score"] >= 70, "risk_level"] = "Low"
    df.loc[df["confidence_score"] < 40, "risk_level"] = "High"

    return df


def process_file(file_path: Path):
    logger.info(f"Generating signals for {file_path.name}")

    df = pd.read_parquet(file_path)
    df = df.sort_values("Date").reset_index(drop=True)

    df = generate_signals(df)

    output_file = SIGNAL_OUTPUT_DIR / file_path.name
    df.to_parquet(output_file, index=False)

    logger.success(f"Saved signals to {output_file.name}")


def run_signal_generation():
    files = list(MARKET_REGIME_DIR.glob("*.parquet"))

    if not files:
        logger.error("No market regime parquet files found")
        return

    for file in files:
        process_file(file)


if __name__ == "__main__":
    logger.info("Starting signal generation")
    run_signal_generation()
