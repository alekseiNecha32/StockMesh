import os
from pathlib import Path
from dotenv import load_dotenv
import boto3
import snowflake.connector

load_dotenv()

TICKERS = ["FITB", "KEY", "RF", "HBAN", "CFG", "MTB"]

LOCAL_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

S3_BUCKET = os.environ.get("AWS_BUCKET", "stockmesh-raw")
S3_PREFIXES = {
    "prices": "prices/",
    "fundamentals": "fundamentals/",
    "news": "news/",
}


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "STOCKMESH_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "STOCKMESH"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def ensure_local_dirs():
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
