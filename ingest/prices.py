"""
Fetch daily OHLCV prices for all tickers via yfinance,
save as Parquet locally, upload to S3, COPY INTO RAW.PRICES from S3.
"""
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

from config import (
    TICKERS,
    LOCAL_DATA_DIR,
    S3_BUCKET,
    S3_PREFIXES,
    get_s3_client,
    get_snowflake_conn,
    ensure_local_dirs,
)

RAW_TABLE = "STOCKMESH.RAW.PRICES"
S3_PREFIX = S3_PREFIXES["prices"]


def fetch_prices(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    raw = yf.download(tickers, start=start, end=end, auto_adjust=True, progress=False)

    if isinstance(raw.columns, pd.MultiIndex):
        raw = raw.stack(level=1).reset_index()
        raw.columns.name = None
        raw = raw.rename(columns={"level_1": "ticker", "Date": "date"})
    else:
        raw = raw.reset_index().rename(columns={"Date": "date"})
        raw["ticker"] = tickers[0]

    raw.columns = [c.lower().replace(" ", "_") for c in raw.columns]
    raw["date"] = pd.to_datetime(raw["date"]).dt.strftime("%Y-%m-%d")
    raw["ingested_at"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return raw[["ticker", "date", "open", "high", "low", "close", "volume", "ingested_at"]]


def save_parquet(df: pd.DataFrame, run_date: str) -> Path:
    ensure_local_dirs()
    path = LOCAL_DATA_DIR / f"prices_{run_date}.parquet"
    df.to_parquet(path, index=False)
    print(f"  Saved {len(df)} rows → {path}")
    return path


def upload_to_s3(local_path: Path, run_date: str) -> str:
    s3 = get_s3_client()
    s3_key = f"{S3_PREFIX}prices_{run_date}.parquet"
    s3.upload_file(str(local_path), S3_BUCKET, s3_key)
    s3_uri = f"s3://{S3_BUCKET}/{S3_PREFIX}"
    print(f"  Uploaded → s3://{S3_BUCKET}/{s3_key}")
    return s3_uri


def copy_into_raw(conn, s3_uri: str) -> None:
    aws_key = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    cursor = conn.cursor()

    # Create external stage pointing to S3 prefix
    cursor.execute(f"""
        CREATE OR REPLACE STAGE STOCKMESH.RAW.PRICES_S3_STAGE
            URL='{s3_uri}'
            CREDENTIALS=(AWS_KEY_ID='{aws_key}' AWS_SECRET_KEY='{aws_secret}')
            FILE_FORMAT=(TYPE=PARQUET)
    """)

    cursor.execute(f"""
        COPY INTO {RAW_TABLE}
        FROM @STOCKMESH.RAW.PRICES_S3_STAGE
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = CONTINUE
    """)
    rows = cursor.fetchone()
    print(f"  COPY INTO {RAW_TABLE}: {rows}")
    cursor.close()


def main():
    end = date.today().isoformat()
    start = (date.today() - timedelta(days=365)).isoformat()
    run_date = date.today().strftime("%Y%m%d")

    print(f"Fetching prices {start} → {end} for {TICKERS}")
    df = fetch_prices(TICKERS, start, end)

    print("Saving Parquet locally...")
    path = save_parquet(df, run_date)

    print("Uploading to S3...")
    s3_uri = upload_to_s3(path, run_date)

    print("Connecting to Snowflake...")
    conn = get_snowflake_conn()
    try:
        copy_into_raw(conn, s3_uri)
    finally:
        conn.close()

    print("Done.")


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent))
    main()
