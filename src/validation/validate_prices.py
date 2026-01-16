import pandas as pd
from pathlib import Path
from loguru import logger

RAW_PRICE_DIR = Path("data/raw/prices")

EXPECTED_PRICE_COLUMNS = ["Open", "High", "Low", "Close", "Adj Close"]
VOLUME_COLUMN = "Volume"


def validate_price_file(file_path: Path):
    logger.info(f"Validating {file_path.name}")

    df = pd.read_csv(file_path)

    # ----------------------------
    # DATA TYPE NORMALIZATION
    # ----------------------------

    # Date parsing
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # ----------------------------
    # BASIC STRUCTURE CHECKS
    # ----------------------------

    if df.empty:
        logger.error("File is empty")
        return

    if df["Date"].isna().any():
        logger.warning("Some dates could not be parsed")

    if df["Date"].duplicated().any():
        logger.warning("Duplicate dates found")

    if not df["Date"].is_monotonic_increasing:
        logger.warning("Dates are not sorted")

    # ----------------------------
    # PRICE COLUMN VALIDATION
    # ----------------------------

    for col in EXPECTED_PRICE_COLUMNS:
        if col not in df.columns:
            logger.warning(f"Missing column: {col}")
            continue

        df[col] = pd.to_numeric(df[col], errors="coerce")

        if (df[col] <= 0).any():
            logger.error(f"Non-positive values found in {col}")

    # ----------------------------
    # VOLUME VALIDATION
    # ----------------------------

    if VOLUME_COLUMN not in df.columns:
        logger.error("Volume column missing")
    else:
        df[VOLUME_COLUMN] = pd.to_numeric(df[VOLUME_COLUMN], errors="coerce")

        if (df[VOLUME_COLUMN] < 0).any():
            logger.error("Negative volume values found")

    logger.success("Basic validation completed")


def run_validation():
    csv_files = list(RAW_PRICE_DIR.glob("*.csv"))

    if not csv_files:
        logger.error("No CSV files found")
        return

    for file_path in csv_files:
        validate_price_file(file_path)


if __name__ == "__main__":
    run_validation()
