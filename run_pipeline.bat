@echo off
echo Starting Market Intelligence Pipeline...

cd /d %~dp0

call .venv\Scripts\activate

python -m src.ingestion.fetch_prices
python -m src.validation.validate_prices
python -m src.transform.normalize_and_save_parquet
python -m src.transform.build_price_features
python -m src.transform.classify_market_regime
python -m src.transform.generate_signals

echo Pipeline completed successfully.
pause
