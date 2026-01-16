import pandas as pd
from pathlib import Path
from loguru import logger

# Input (raw) and output (processed) directories
RAW_PRICE_DIR = Path("data/raw/prices")
PROCESSED_PRICE_DIR = Path("data/processed/prices")

PROCESSED_PRICE_DIR.mkdir(parents=True, exist_ok=True)


def process_price_file(file_path: Path):
    logger.info(f"Processing {file_path.name}")

    # Read raw CSV
    df = pd.read_csv(file_path)

    # 1. Convert Date to datetime
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # 2. Drop rows with invalid dates
    df = df.dropna(subset=["Date"])

    # 3. Sort by Date (time normalization)
    df = df.sort_values("Date")

    # 4. Reset index after sorting
    df = df.reset_index(drop=True)

    # 5. Convert numeric columns safely
    numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 6. Save cleaned data as Parquet
    output_file = PROCESSED_PRICE_DIR / file_path.with_suffix(".parquet").name
    df.to_parquet(output_file, index=False)

    logger.success(f"Saved cleaned data to {output_file.name}")


def run_processing():
    csv_files = list(RAW_PRICE_DIR.glob("*.csv"))

    if not csv_files:
        logger.error("No raw CSV files found")
        return

    for file_path in csv_files:
        process_price_file(file_path)


if __name__ == "__main__":
    run_processing()
