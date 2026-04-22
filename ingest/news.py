"""
Fetch recent news headlines for all tickers via yfinance Ticker.news,
save as Parquet locally, upload to S3, COPY INTO RAW.NEWS from S3.
"""
import os
import sys
from datetime import date
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

RAW_TABLE = "STOCKMESH.RAW.NEWS"
S3_PREFIX = S3_PREFIXES["news"]


def fetch_news(tickers: list[str]) -> pd.DataFrame:
    rows = []
    for ticker in tickers:
        articles = yf.Ticker(ticker).news
        for article in articles:
            content = article.get("content", {})
            rows.append({
                "uuid": article.get("id") or content.get("id"),
                "ticker": ticker,
                "title": content.get("title") or article.get("title"),
                "publisher": content.get("provider", {}).get("displayName") or article.get("publisher"),
                "published_at": pd.to_datetime(
                    content.get("pubDate") or article.get("providerPublishTime"), utc=True, errors="coerce"
                ).strftime("%Y-%m-%d %H:%M:%S%z") if pd.to_datetime(
                    content.get("pubDate") or article.get("providerPublishTime"), utc=True, errors="coerce"
                ) is not pd.NaT else None,
                "link": (content.get("canonicalUrl") or {}).get("url") or article.get("link"),
                "ingested_at": pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            })
        print(f"  Fetched {ticker}: {len(articles)} articles")

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["uuid", "ticker"])
    return df


def save_parquet(df: pd.DataFrame, run_date: str) -> Path:
    ensure_local_dirs()
    path = LOCAL_DATA_DIR / f"news_{run_date}.parquet"
    df.to_parquet(path, index=False)
    print(f"  Saved {len(df)} rows → {path}")
    return path


def upload_to_s3(local_path: Path, run_date: str) -> str:
    s3 = get_s3_client()
    s3_key = f"{S3_PREFIX}news_{run_date}.parquet"
    s3.upload_file(str(local_path), S3_BUCKET, s3_key)
    s3_uri = f"s3://{S3_BUCKET}/{S3_PREFIX}"
    print(f"  Uploaded → s3://{S3_BUCKET}/{s3_key}")
    return s3_uri


def copy_into_raw(conn, s3_uri: str) -> None:
    aws_key = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    cursor = conn.cursor()

    cursor.execute(f"""
        CREATE OR REPLACE STAGE STOCKMESH.RAW.NEWS_S3_STAGE
            URL='{s3_uri}'
            CREDENTIALS=(AWS_KEY_ID='{aws_key}' AWS_SECRET_KEY='{aws_secret}')
            FILE_FORMAT=(TYPE=PARQUET)
    """)

    cursor.execute(f"""
        COPY INTO {RAW_TABLE}
        FROM @STOCKMESH.RAW.NEWS_S3_STAGE
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = CONTINUE
    """)
    rows = cursor.fetchone()
    print(f"  COPY INTO {RAW_TABLE}: {rows}")
    cursor.close()


def main():
    run_date = date.today().strftime("%Y%m%d")

    print(f"Fetching news for {TICKERS}")
    df = fetch_news(TICKERS)

    if df.empty:
        print("No news articles found. Exiting.")
        return

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
