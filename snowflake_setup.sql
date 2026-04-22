-- =============================================================
-- StockMesh Snowflake Setup
-- Run this entire script once in your Snowflake worksheet.
-- =============================================================

-- ------------------------------------------------------------
-- 1. Warehouse
-- ------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS STOCKMESH_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'StockMesh compute warehouse';

USE WAREHOUSE STOCKMESH_WH;

-- ------------------------------------------------------------
-- 2. Database & Schemas
-- ------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS STOCKMESH;
USE DATABASE STOCKMESH;

CREATE SCHEMA IF NOT EXISTS STOCKMESH.RAW
    COMMENT = 'Raw ingested data from yfinance via Parquet';

CREATE SCHEMA IF NOT EXISTS STOCKMESH.DBT_DEV
    COMMENT = 'dbt transformation output (staging + mart models)';

-- ------------------------------------------------------------
-- 3. Internal Stage (replaces S3 for now)
-- User home stage: @~/stockmesh/
-- No extra setup needed — Snowflake creates @~ per user automatically.
-- Sub-paths are created implicitly by PUT commands.
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- 4. Raw Tables
-- All columns VARCHAR/FLOAT/etc. to accept dirty ingest data.
-- dbt staging models handle type casting and cleaning.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS STOCKMESH.RAW.PRICES (
    ticker          VARCHAR(10)     NOT NULL,
    date            DATE            NOT NULL,
    open            FLOAT,
    high            FLOAT,
    low             FLOAT,
    close           FLOAT,
    volume          BIGINT,
    ingested_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS STOCKMESH.RAW.FUNDAMENTALS (
    ticker                  VARCHAR(10)     NOT NULL,
    market_cap              FLOAT,
    trailing_p_e            FLOAT,
    forward_p_e             FLOAT,
    price_to_book           FLOAT,
    trailing_eps            FLOAT,
    forward_eps             FLOAT,
    dividend_yield          FLOAT,
    payout_ratio            FLOAT,
    return_on_equity        FLOAT,
    return_on_assets        FLOAT,
    debt_to_equity          FLOAT,
    current_ratio           FLOAT,
    sector                  VARCHAR(100),
    industry                VARCHAR(200),
    full_time_employees     BIGINT,
    long_name               VARCHAR(300),
    current_price           FLOAT,
    fifty_two_week_high     FLOAT,
    fifty_two_week_low      FLOAT,
    ingested_at             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS STOCKMESH.RAW.NEWS (
    uuid            VARCHAR(200)    NOT NULL,
    ticker          VARCHAR(10)     NOT NULL,
    title           VARCHAR(1000),
    publisher       VARCHAR(200),
    published_at    TIMESTAMP_TZ,
    link            VARCHAR(2000),
    ingested_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ------------------------------------------------------------
-- 5. Grants (adjust role as needed)
-- ------------------------------------------------------------
GRANT USAGE ON DATABASE STOCKMESH TO ROLE SYSADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE STOCKMESH TO ROLE SYSADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA STOCKMESH.RAW TO ROLE SYSADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA STOCKMESH.DBT_DEV TO ROLE SYSADMIN;

-- Future tables created by dbt
GRANT ALL ON FUTURE TABLES IN SCHEMA STOCKMESH.DBT_DEV TO ROLE SYSADMIN;
GRANT CREATE TABLE ON SCHEMA STOCKMESH.DBT_DEV TO ROLE SYSADMIN;
GRANT CREATE VIEW ON SCHEMA STOCKMESH.DBT_DEV TO ROLE SYSADMIN;

-- ------------------------------------------------------------
-- 6. Verify setup
-- ------------------------------------------------------------
SHOW WAREHOUSES LIKE 'STOCKMESH_WH';
SHOW SCHEMAS IN DATABASE STOCKMESH;
SHOW TABLES IN SCHEMA STOCKMESH.RAW;
