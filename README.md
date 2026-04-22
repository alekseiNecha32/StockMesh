# StockMesh

A stock market data warehouse built on Snowflake, transformed by dbt, and visualized in Streamlit.

**Focus**: Fifth Third Bank (FITB) vs. regional bank peers — KEY, RF, HBAN, CFG, MTB.

## Stack

| Layer | Tool |
|-------|------|
| Data pull | yfinance (Python) |
| Local storage | Parquet files (`data/raw/`) |
| Cloud warehouse | Snowflake (internal stage → raw tables) |
| Transformations | dbt |
| Dashboard | Streamlit + Plotly |

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp .env.example .env
# Edit .env with your Snowflake account details
```

Your Snowflake account identifier looks like `abc12345.us-east-1` (find it in Snowflake UI → bottom-left account menu).

### 3. Set up Snowflake

Run the setup SQL in your Snowflake worksheet:

```bash
# Copy contents of snowflake_setup.sql into Snowflake UI and execute
```

### 4. Run ingestion

```bash
python ingest/prices.py
python ingest/fundamentals.py
python ingest/news.py
```

Each script: fetches data → saves Parquet locally → PUTs to Snowflake internal stage → COPYs into raw table.

### 5. Run dbt transformations

```bash
cd dbt_project
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your credentials

dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve
```

### 6. Launch dashboard

```bash
streamlit run app/dashboard.py
```

## Project Structure

```
StockMesh/
├── ingest/          # Python ingestion scripts (prices, fundamentals, news)
├── snowflake_setup.sql
├── dbt_project/
│   └── models/
│       ├── prices/
│       ├── fundamentals/
│       ├── news/
│       └── cross_domain/   # fitb_peer_scorecard
└── app/
    └── dashboard.py
```

## Domains

See [DOMAINS.md](DOMAINS.md) for a full description of each data domain.
