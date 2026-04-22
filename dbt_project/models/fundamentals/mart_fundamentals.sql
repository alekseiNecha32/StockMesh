with base as (
    select * from {{ ref('stg_fundamentals') }}
)

select
    ticker,
    company_name,
    sector,
    industry,
    current_price,
    market_cap,
    trailing_pe,
    forward_pe,
    price_to_book,
    trailing_eps,
    forward_eps,
    dividend_yield,
    payout_ratio,
    return_on_equity,
    return_on_assets,
    debt_to_equity,
    current_ratio,
    full_time_employees,
    week_52_high,
    week_52_low,
    round((current_price - week_52_low) / nullif(week_52_high - week_52_low, 0) * 100, 1)
                            as week_52_position_pct,
    ingested_at
from base
