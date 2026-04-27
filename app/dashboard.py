import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

st.set_page_config(
    page_title="StockMesh — FITB Peer Scorecard",
    page_icon="📈",
    layout="wide",
)


@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "STOCKMESH_WH"),
        database="STOCKMESH",
        schema="DBT_DEV_DBT_DEV",
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    )


@st.cache_data(ttl=3600)
def load_scorecard() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM STOCKMESH.DBT_DEV_DBT_DEV.FITB_PEER_SCORECARD", conn)
    df.columns = [c.lower() for c in df.columns]
    return df


@st.cache_data(ttl=3600)
def load_returns_history() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql("""
        SELECT ticker, date, cumulative_return
        FROM STOCKMESH.DBT_DEV_DBT_DEV.MART_DAILY_RETURNS
        ORDER BY ticker, date
    """, conn)
    df.columns = [c.lower() for c in df.columns]
    return df


# ── Header ────────────────────────────────────────────────────────────────────
st.title("StockMesh — Regional Bank Scorecard")

with st.expander("About this dashboard"):
    st.markdown("""
**StockMesh** is a data pipeline that automatically collects stock market data for 6 regional banks, stores it in the cloud, and displays it in a live dashboard.

**What it does:**
1. Downloads stock prices, company financials, and news headlines from Yahoo Finance
2. Saves the data to AWS S3 (cloud storage)
3. Loads it into Snowflake (cloud database)
4. Cleans and transforms it using dbt
5. Displays it in this Streamlit dashboard

**What you can see:**
- How much each bank's stock gained over 30 days and 1 year
- Valuation metrics (P/E, P/B, ROE) to compare if a stock is cheap or expensive
- Whether recent news about each bank is positive or negative

""")

try:
    scorecard = load_scorecard()
    history = load_returns_history()
except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.info("Make sure your .env file is configured and dbt models have been run.")
    st.stop()

# ── Section 1: Scorecard Table ────────────────────────────────────────────────
st.subheader("Scorecard")

display_cols = {
    "ticker": "Ticker",
    "company_name": "Company",
    "current_price": "Price ($)",
    "return_30d": "30d Return",
    "cumulative_return": "1Y Return",
    "volatility_30d": "Volatility",
    "trailing_pe": "P/E",
    "price_to_book": "P/B",
    "return_on_equity": "ROE",
    "avg_sentiment": "Sentiment",
}

table_df = scorecard[list(display_cols.keys())].copy()
table_df = table_df.rename(columns=display_cols)

pct_cols = ["30d Return", "1Y Return", "Volatility", "ROE"]
for col in pct_cols:
    if col in table_df.columns:
        table_df[col] = table_df[col].apply(
            lambda x: f"{x*100:+.1f}%" if pd.notna(x) else "—"
        )

for col in ["P/E", "P/B"]:
    if col in table_df.columns:
        table_df[col] = table_df[col].apply(
            lambda x: f"{x:.1f}x" if pd.notna(x) else "—"
        )

table_df["Sentiment"] = table_df["Sentiment"].apply(
    lambda x: f"{x:+.2f}" if pd.notna(x) else "—"
)

fitb_idx = table_df[table_df["Ticker"] == "FITB"].index.tolist()

def highlight_fitb(row):
    if row["Ticker"] == "FITB":
        return ["background-color: #1a3a5c; color: white; font-weight: bold"] * len(row)
    return [""] * len(row)

st.dataframe(
    table_df.style.apply(highlight_fitb, axis=1),
    use_container_width=True,
    hide_index=True,
)

# ── Section 2: Cumulative Return Chart ───────────────────────────────────────
st.subheader("Cumulative Return — 1 Year")

history["cumulative_pct"] = history["cumulative_return"] * 100

fig_line = px.line(
    history,
    x="date",
    y="cumulative_pct",
    color="ticker",
    color_discrete_map={"FITB": "#e63946"},
    labels={"cumulative_pct": "Cumulative Return (%)", "date": "Date", "ticker": "Ticker"},
)
fig_line.update_layout(
    hovermode="x unified",
    legend_title="Ticker",
    plot_bgcolor="#0e1117",
    paper_bgcolor="#0e1117",
    font_color="#fafafa",
    yaxis=dict(ticksuffix="%"),
)
st.plotly_chart(fig_line, use_container_width=True)

# ── Section 3: Sentiment Bar Chart ───────────────────────────────────────────
st.subheader("News Sentiment Score")

sentiment_df = scorecard[["ticker", "avg_sentiment", "bullish_count", "bearish_count", "news_count"]].copy()
sentiment_df = sentiment_df.sort_values("avg_sentiment", ascending=False)

colors = ["#e63946" if t == "FITB" else "#457b9d" for t in sentiment_df["ticker"]]

fig_bar = go.Figure(go.Bar(
    x=sentiment_df["ticker"],
    y=sentiment_df["avg_sentiment"],
    marker_color=colors,
    text=sentiment_df["avg_sentiment"].apply(lambda x: f"{x:+.2f}" if pd.notna(x) else "—"),
    textposition="outside",
    hovertemplate=(
        "<b>%{x}</b><br>"
        "Avg Sentiment: %{y:.2f}<br>"
        "<extra></extra>"
    ),
))
fig_bar.update_layout(
    yaxis=dict(range=[-1.1, 1.1], title="Avg Sentiment Score"),
    xaxis_title="Ticker",
    plot_bgcolor="#0e1117",
    paper_bgcolor="#0e1117",
    font_color="#fafafa",
    shapes=[dict(type="line", x0=-0.5, x1=len(sentiment_df)-0.5, y0=0, y1=0,
                 line=dict(color="gray", dash="dash", width=1))],
)
st.plotly_chart(fig_bar, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Tickers tracked", len(scorecard))
with col2:
    total_news = int(scorecard["news_count"].sum()) if "news_count" in scorecard.columns else 0
    st.metric("News articles", total_news)
with col3:
    st.caption("Data: Yahoo Finance via yfinance · Warehouse: Snowflake · Transforms: dbt")
