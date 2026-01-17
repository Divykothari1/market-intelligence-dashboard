import sys
from pathlib import Path
from datetime import datetime
import pytz

# -------------------------------------------------
# PROJECT ROOT & PATH FIX
# -------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from src.news.impact_engine import generate_impact_explanation

# -------------------------------------------------
# PATHS (DEFINE EARLY!)
# -------------------------------------------------
SIGNAL_DIR = Path("data/processed/signals")
NEWS_DIR = Path("data/processed/news")

# -------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------
st.set_page_config(
    page_title="Market Intelligence Dashboard",
    layout="wide"
)

st.title("📊 Market Intelligence Dashboard (Daily)")
st.caption("End-of-day market regime, signals, and news intelligence")

# =================================================
# 🕒 LAST UPDATED + PIPELINE SOURCE
# =================================================
IST = pytz.timezone("Asia/Kolkata")

def get_last_updated_time(signal_dir: Path):
    files = list(signal_dir.glob("*.parquet"))
    if not files:
        return None
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    ts = datetime.fromtimestamp(latest_file.stat().st_mtime, tz=IST)
    return ts.strftime("%d %b %Y, %I:%M %p IST")

last_updated = get_last_updated_time(SIGNAL_DIR)

if last_updated:
    st.caption(
        f"🕒 **Last updated:** {last_updated}  |  "
        f"⚙️ **Source:** GitHub Actions (Automated EOD Pipeline)"
    )
else:
    st.caption("🕒 Data not updated yet (pipeline not run or market closed)")

# =================================================
# LOAD SIGNAL DATA (OVERVIEW)
# =================================================
@st.cache_data
def load_signal_data():
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

df_overview = load_signal_data()

# =================================================
# 📌 MARKET OVERVIEW
# =================================================
st.subheader("📌 Market Overview")

if df_overview.empty:
    st.warning(
        "⚠️ No signal data available yet.\n\n"
        "**Possible reasons:**\n"
        "- Weekend / market holiday\n"
        "- First-time setup\n"
        "- Pipeline just ran and data is processing\n\n"
        "✅ The system is automated and will update after the next market close."
    )
    st.stop()
else:
    st.success("🟢 Pipeline status: Healthy (latest run successful)")

st.dataframe(df_overview.sort_values("Stock"), use_container_width=True)

# =================================================
# 📈 STOCK SELECTION
# =================================================
st.divider()
st.subheader("📈 Stock Detail View")

selected_stock = st.selectbox(
    "Select a stock",
    df_overview["Stock"].sort_values().unique()
)

# =================================================
# LOAD STOCK DATA
# =================================================
stock_file = SIGNAL_DIR / f"{selected_stock}.NS.parquet"
df_stock = pd.read_parquet(stock_file).sort_values("Date")

latest = df_stock.iloc[-1]
latest_date = pd.to_datetime(latest["Date"]).date()

# =================================================
# 🧾 STOCK REPORT HEADER
# =================================================
st.divider()
st.header("🧾 Stock Intelligence Report")
st.caption(
    f"Stock: **{selected_stock}** | "
    f"Data as of: **{latest_date} (Market Close)**"
)

# =================================================
# 📰 LATEST NEWS (LAST 7 DAYS)
# =================================================
st.divider()
st.subheader("📰 Latest News (Last 7 Days)")

news_file = NEWS_DIR / f"{selected_stock}.parquet"

latest_sentiment = "Neutral"
latest_confidence = 0
df_news = pd.DataFrame()

if not news_file.exists():
    st.info("No significant news found in the last 7 days.")
else:
    df_news = pd.read_parquet(news_file)

    if df_news.empty:
        st.info("No significant news found in the last 7 days.")
    else:
        df_news["date"] = pd.to_datetime(df_news["date"])
        df_news = df_news.sort_values("date", ascending=False).head(5)

        latest_sentiment = df_news.iloc[0].get("sentiment", "Neutral")
        latest_confidence = int(df_news.iloc[0].get("confidence", 0))

        for _, row in df_news.iterrows():
            sentiment = row.get("sentiment", "Neutral")
            confidence = int(row.get("confidence", 0))
            dot = "🟢" if sentiment == "Positive" else "🔴" if sentiment == "Negative" else "🟡"

            st.markdown(
                f"""
<div style="font-size:14px; line-height:1.4">
<b>{dot} {row['headline']}</b><br>
<span style="color:#9aa0a6">
Source: <i>{row['source']}</i> |
Sentiment: <b>{sentiment}</b> ({confidence}%)
</span><br>
<a href="{row['url']}" target="_blank">Read full article</a>
</div>
<hr style="margin:6px 0;">
""",
                unsafe_allow_html=True
            )

# =================================================
# 🧠 NEWS IMPACT ASSESSMENT
# =================================================
st.subheader("🧠 News Impact Assessment")

impact_text = generate_impact_explanation(
    sentiment=latest_sentiment,
    confidence=latest_confidence,
    market_regime=latest["market_regime"],
    signal=latest["signal_label"]
)

st.info(impact_text)

# =================================================
# 🔗 SIGNAL ALIGNMENT
# =================================================
st.subheader("🔗 Signal Alignment")

technical_signal = latest["signal_label"]
news_sentiments = df_news["sentiment"].value_counts() if not df_news.empty else {}

positive = news_sentiments.get("Positive", 0)
negative = news_sentiments.get("Negative", 0)

if technical_signal == "Bullish" and positive > negative:
    alignment, color = "Aligned", "🟢"
elif technical_signal == "Bearish" and negative > positive:
    alignment, color = "Aligned", "🟢"
elif positive > 0 and negative > 0:
    alignment, color = "Mixed", "🟡"
else:
    alignment, color = "Conflict", "🔴"

st.markdown(
    f"<div style='font-size:16px;'><b>{color} Signal Alignment:</b> {alignment}</div>",
    unsafe_allow_html=True
)

# =================================================
# 📌 SNAPSHOT METRICS
# =================================================
st.subheader("📌 Current Market Snapshot")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Market Regime", latest["market_regime"])
col2.metric("Signal", latest["signal_label"])
col3.metric("Confidence", f"{int(latest['confidence_score'])}%")
col4.metric("Risk Level", latest["risk_level"])

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
# 📈 EXPECTED MOVE
# =================================================
st.info(f"📈 **Expected Move (next ~5 days):** {latest['expected_move_pct']}%")

# =================================================
# 🧠 SIGNAL EXPLANATION
# =================================================
st.subheader("🧠 Signal Explanation")

price_position = "above" if latest["Close"] > latest["sma_20"] else "below"

st.write(f"- **Market Regime:** {latest['market_regime']}")
st.write(f"- **Signal:** {latest['signal_label']} ({latest['signal_strength']})")
st.write(f"- Price is **{price_position}** the 20-day moving average.")
st.write("- Signal combines trend, volatility, and momentum conditions.")

# =================================================
# ⚠️ DISCLAIMER & CREDIBILITY
# =================================================
st.divider()
st.caption(
    "ℹ️ **Disclaimer:** Educational & analytical use only. Not investment advice.\n\n"
    "**Data Sources:** Yahoo Finance, News APIs  |  "
    "**Automation:** GitHub Actions  |  "
    "**Hosting:** Streamlit Cloud"
)
