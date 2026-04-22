# StockMesh Domains

Three data domains feed into a unified scorecard comparing Fifth Third Bank (FITB) against regional bank peers.

---

## Domain 1: Prices

**Source**: `yfinance.download()` — daily OHLCV data  
**Raw table**: `STOCKMESH.RAW.PRICES`  
**Tickers**: FITB, KEY, RF, HBAN, CFG, MTB  
**Lookback**: 1 year rolling

### Fields
| Field | Description |
|-------|-------------|
| ticker | Stock symbol |
| date | Trading date |
| open | Opening price |
| high | Intraday high |
| low | Intraday low |
| close | Closing price (adjusted) |
| volume | Shares traded |

### dbt Models
- `stg_prices` — type casting, null filtering, deduplication
- `mart_daily_returns` — daily % return, 30-day rolling volatility, cumulative return

---

## Domain 2: Fundamentals

**Source**: `yfinance.Ticker.info` — company financials snapshot  
**Raw table**: `STOCKMESH.RAW.FUNDAMENTALS`  
**Refresh**: Daily (latest available values)

### Fields
| Field | Description |
|-------|-------------|
| ticker | Stock symbol |
| market_cap | Market capitalization |
| trailing_pe | Price-to-earnings (trailing 12m) |
| price_to_book | Price-to-book ratio |
| trailing_eps | Earnings per share |
| dividend_yield | Annual dividend yield |
| sector | GICS sector |
| industry | Industry group |
| full_time_employees | Headcount |
| ingested_at | Snapshot timestamp |

### dbt Models
- `stg_fundamentals` — normalize nulls, cast types
- `mart_fundamentals` — clean ratio table for scorecard

---

## Domain 3: News

**Source**: `yfinance.Ticker.news` — recent news headlines  
**Raw table**: `STOCKMESH.RAW.NEWS`  
**Refresh**: Daily

### Fields
| Field | Description |
|-------|-------------|
| uuid | Unique article ID |
| ticker | Stock symbol queried |
| title | Article headline |
| publisher | News source |
| published_at | Publication timestamp |
| link | Article URL |

### dbt Models
- `stg_news` — dedupe by uuid, parse timestamps
- `mart_news_sentiment` — keyword-based sentiment score (-1 bearish → +1 bullish)

---

## Cross-Domain: FITB Peer Scorecard

**Model**: `fitb_peer_scorecard`  
**Output table**: `STOCKMESH.DBT_DEV.FITB_PEER_SCORECARD`

Joins all three mart tables on `ticker` to produce one row per bank with:

| Column | Source |
|--------|--------|
| ticker | — |
| is_focus | TRUE for FITB |
| return_30d | mart_daily_returns |
| volatility_30d | mart_daily_returns |
| cumulative_return_1y | mart_daily_returns |
| trailing_pe | mart_fundamentals |
| price_to_book | mart_fundamentals |
| market_cap | mart_fundamentals |
| avg_sentiment | mart_news_sentiment |
| news_count | mart_news_sentiment |
