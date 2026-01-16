import subprocess
import sys

def run(cmd):
    print(f"\n▶ Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        sys.exit(result.returncode)

if __name__ == "__main__":
    run("python src/ingestion/fetch_prices.py")
    run("python src/news/fetch_news.py")
    run("python src/news/analyze_sentiment.py")
    run("python src/transform/build_price_features.py")
    run("python src/transform/classify_market_regime.py")
    run("python src/transform/generate_signals.py")
    print("\n✅ Market Intelligence Pipeline completed successfully")
