import sys
from pathlib import Path
from datetime import datetime
import pytz

# =================================================
# PROJECT ROOT & PATH FIX
# =================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from src.news.impact_engine import generate_impact_explanation
from src.config.nifty50_symbols import NIFTY_50_SYMBOLS

# =================================================
# PATHS
# =================================================
SIGNAL_DIR = Path("data/processed/signals")
NEWS_DIR = Path("data/processed/news")

# =================================================
# STREAMLIT CONFIG
# =================================================
st.set_page_config(
    page_title="Market Intelligence Dashboard",
    layout="wide"
)

st.title("📊 Market Intelligence Dashboard (Daily)")
st.caption("End-of-day market regime, signals, and news intelligence")

# =================================================
# 🔄 MANUAL REFRESH
# =================================================
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# =================================================
# 🕒 PIPELINE LAST RUN TIME
# =================================================
IST = pytz.timezone("Asia/Kolkata")

def get_last_updated_time():
    files = list(SIGNAL_DIR.glob("*.parquet"))
    if not files:
        return None
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    return datetime.fromtimestamp(latest_file.stat().st_mtime, tz=IST)

last_updated_ts = get_last_updated_time()

if last_updated_ts:
    st.caption(
        f"🕒 **Pipeline run:** {last_updated_ts.strftime('%d %b %Y, %I:%M %p IST')}  |  "
        f"⚙️ **Source:** GitHub Actions (Automated EOD)"
    )
else:
    st.caption("🕒 Data not updated yet")

# =================================================
# CACHE INVALIDATION KEY
# =================================================
def get_latest_signal_timestamp():
    files = list(SIGNAL_DIR.glob("*.parquet"))
    if not files:
        return 0
    return max(f.stat().st_mtime for f in files)

LATEST_TS = get_latest_signal_timestamp()

# =================================================
# LOAD OVERVIEW DATA (NIFTY 50 ENFORCED)
# =================================================
@st.cache_data(show_spinner=False)
def load_signal_data(latest_ts):
    _ = latest_ts  # cache dependency

    rows = []
    available = set()

    for symbol in NIFTY_50_SYMBOLS:
        file_path = SIGNAL_DIR / f"{symbol}.parquet"
        if not file_path.exists():
            continue

        df = pd.read_parquet(file_path)
        if df.empty:
            continue

        df = df.sort_values("Date")
        latest = df.iloc[-1]
        available.add(symbol)

        rows.append({
            "Stock": symbol.replace(".NS", ""),
            "Date": pd.to_datetime(latest["Date"]).date(),
            "Market Regime": latest["market_regime"],
            "Signal": latest["signal_label"],
            "Strength": latest["signal_strength"]
        })

    missing = sorted(set(NIFTY_50_SYMBOLS) - available)
    return pd.DataFrame(rows), missing

df_overview, missing_stocks = load_signal_data(LATEST_TS)

# =================================================
# 📌 MARKET OVERVIEW
# =================================================
st.subheader("📌 Market Overview")

if df_overview.empty:
    st.warning("⚠️ No signal data available yet.")
    st.stop()

df_overview = df_overview.sort_values(
    by=["Date", "Stock"],
    ascending=[False, True]
)

latest_date = df_overview["Date"].max()
oldest_date = df_overview["Date"].min()

st.success("🟢 Pipeline status: Healthy")

st.info(
    f"📅 **Data coverage:** {oldest_date} → {latest_date}\n\n"
    f"📊 **Coverage:** {len(df_overview)}/{len(NIFTY_50_SYMBOLS)} NIFTY stocks\n\n"
    "ℹ️ Some stocks may show older dates due to non-trading days or data availability."
)

if missing_stocks:
    st.warning(
        "⚠️ Missing stocks: "
        + ", ".join(s.replace(".NS", "") for s in missing_stocks)
    )

st.dataframe(df_overview, use_container_width=True)

# =================================================
# 📈 STOCK DETAIL
# =================================================
st.divider()
st.subheader("📈 Stock Detail View")

selected_stock = st.selectbox(
    "Select a stock",
    df_overview["Stock"].unique()
)

stock_file = SIGNAL_DIR / f"{selected_stock}.NS.parquet"
df_stock = pd.read_parquet(stock_file).sort_values("Date")
latest = df_stock.iloc[-1]

# =================================================
# 📰 NEWS
# =================================================
st.divider()
st.subheader("📰 Latest News (Last 7 Days)")

news_file = NEWS_DIR / f"{selected_stock}.parquet"
df_news = pd.DataFrame()
latest_sentiment, latest_confidence = "Neutral", 0

if news_file.exists():
    df_news = pd.read_parquet(news_file)

if df_news.empty:
    st.info("No significant news found.")
else:
    df_news["date"] = pd.to_datetime(df_news["date"])
    df_news = df_news.sort_values("date", ascending=False).head(5)

    latest_sentiment = df_news.iloc[0]["sentiment"]
    latest_confidence = int(df_news.iloc[0]["confidence"])

    for _, row in df_news.iterrows():
        dot = "🟢" if row["sentiment"] == "Positive" else "🔴" if row["sentiment"] == "Negative" else "🟡"
        st.markdown(
            f"""
<b>{dot} {row['headline']}</b><br>
<i>{row['source']}</i> | {row['sentiment']} ({int(row['confidence'])}%)<br>
<a href="{row['url']}" target="_blank">Read article</a>
<hr>
""",
            unsafe_allow_html=True
        )

# =================================================
# 🧠 NEWS IMPACT
# =================================================
st.subheader("🧠 News Impact Assessment")
st.info(generate_impact_explanation(
    sentiment=latest_sentiment,
    confidence=latest_confidence,
    market_regime=latest["market_regime"],
    signal=latest["signal_label"]
))

# =================================================
# 📈 PRICE CHART
# =================================================
st.subheader("📈 Price Chart")

chart_type = st.radio(
    "Chart Type",
    ["Line Chart", "Candlestick"],
    horizontal=True
)

fig = go.Figure()

if chart_type == "Line Chart":
    fig.add_trace(go.Scatter(x=df_stock["Date"], y=df_stock["Close"], name="Close"))
    fig.add_trace(go.Scatter(x=df_stock["Date"], y=df_stock["sma_20"], name="SMA 20"))
    fig.add_trace(go.Scatter(x=df_stock["Date"], y=df_stock["sma_50"], name="SMA 50"))
else:
    fig.add_trace(go.Candlestick(
        x=df_stock["Date"],
        open=df_stock["Open"],
        high=df_stock["High"],
        low=df_stock["Low"],
        close=df_stock["Close"]
    ))

fig.update_layout(height=500, xaxis_rangeslider_visible=False)
st.plotly_chart(fig, use_container_width=True)

# =================================================
# DISCLAIMER
# =================================================
st.divider()
st.caption(
    "ℹ️ Educational use only. Not investment advice.\n\n"
    "Data: Yahoo Finance | News APIs\n"
    "Automation: GitHub Actions | Hosting: Streamlit Cloud"
)
