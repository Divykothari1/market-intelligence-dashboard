📊 Market Intelligence System
Introduction

Market Intelligence System is an end-to-end data engineering + analytics project that processes daily Indian stock market data to generate technical signals, market regimes, and news-driven sentiment insights, presented through an interactive dashboard.

The system is designed to automatically update after market close, simulating how real-world financial analytics pipelines operate.

This project was built to learn and apply data pipelines, automation, NLP, and dashboarding concepts in a production-like setup.

🚀 Key Features

📅 Automated End-of-Day (EOD) Pipeline
Runs daily using GitHub Actions to fetch and process the latest market data.

📈 Market Regime & Technical Signal Detection
Identifies bullish, bearish, or sideways regimes using price-based indicators.

📰 News Fetching & Sentiment Analysis
Fetches recent financial news and analyzes sentiment using NLP (VADER).

🧠 AI-Based Impact Explanation
Generates natural-language explanations combining price signals + news sentiment.

📊 Interactive Dashboard (Streamlit)
Real-time visualization with stock overview, detailed charts, and news insights.

🔁 Automatic Data Refresh
Dashboard always reflects the latest available EOD data when opened.

🛠 Tech Stack

Language: Python

Data Processing: Pandas, NumPy

Visualization: Streamlit, Plotly

Data Source: Yahoo Finance, News APIs

NLP: NLTK VADER (Sentiment Analysis)

Automation: GitHub Actions (Scheduled Jobs)

Deployment: Streamlit Cloud

🌐 Live Demo & Code

Live Dashboard:
👉 https://market-intelligence-dashboard-aqfw4mgasg6satenpvuou4.streamlit.app

⚠️ Disclaimer

This project is built strictly for educational and analytical purposes.
It is not financial or investment advice.
