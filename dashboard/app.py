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
# 🔄 MANUAL REFRESH (CRITICAL)
# =================================================
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# =================================================
# 🕒 LAST UPDATED (EOD)
# =================================================
IST = pytz.timezone("Asia/Kolkata")

def get_last_updated_time():
    files = list(SIGNAL_DIR.glob("*.parquet"))
    if not files:
        return None
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    ts = datetime.fromtimestamp(latest_file.stat().st_mtime, tz=IST)
    return ts.strftime("%d %b %Y, %I:%M %p IST")

last_updated = get_last_updated_time()

if last_updated:
    st.caption(
        f"🕒 **Last updated:** {last_updated}  |  "
        f"⚙️ **Source:** GitHub Actions (Automated EOD Pipeline)"
    )
else:
    st.caption("🕒 Data not updated yet (pipeline not run or market closed)")

# =================================================
# CACHE INVALIDATION KEY (IMPORTANT)
# =================================================
def get_latest_signal_timestamp():
    files = list(SIGNAL_DIR.glob("*.parquet"))
    if not files:
        return 0
    return max(f.stat().st_mtime for f in files)

LATEST_TS = get_latest_signal_timestamp()

# =================================================
# LOAD SIGNAL DATA (OVERVIEW)
# =================================================
@st.cache_data(show_spinner=False)
def load_signal_data(latest_ts):
    rows = []

    for file_path in SIGNAL_DIR.glob("*.parquet"):
        df = pd.read_parquet(file_path)
        if df.empty:
            continue

        df = df.sort_values("Date")
        latest = df.iloc[-1]

        rows.append({
            "Stock": file_path.stem.replace(".NS", ""),
            "Date": pd.to_datetime(latest["Date"]).date(),
            "Market Regime": latest["market_regime"],
            "Signal": latest["signal_label"],
            "Strength": latest["signal_strength"]
        })

    return pd.DataFrame(rows)

df_overview = load_signal_data(LATEST_TS)

# =================================================
# 📌 MARKET OVERVIEW
# =================================================
st.subheader("📌 Market Overview")

if df_overview.empty:
    st.warning(
        "⚠️ No signal data available yet.\n\n"
        "- Weekend / holiday\n"
        "- First-time setup\n"
        "- Pipeline still running\n\n"
        "✅ System is fully automated."
    )
    st.stop()

st.success("🟢 Pipeline status: Healthy")
st.dataframe(df_overview.sort_values("Stock"), use_container_width=True)

# =================================================
# 📈 STOCK DETAIL
# =================================================
st.divider()
st.subheader("📈 Stock Detail View")

selected_stock = st.selectbox(
    "Select a stock",
    df_overview["Stock"].sort_values().unique()
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
latest_sentiment, latest_confidence = "Neutral", 0
df_news = pd.DataFrame()

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
