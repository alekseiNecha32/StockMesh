with prices as (
    select * from {{ ref('stg_prices') }}
),

-- Step 1: compute daily return using LAG (no nesting)
with_lag as (
    select
        ticker,
        date,
        close,
        volume,
        lag(close) over (partition by ticker order by date) as prev_close,
        first_value(close) over (
            partition by ticker order by date
            rows between unbounded preceding and current row
        ) as first_close
    from prices
),

with_returns as (
    select
        ticker,
        date,
        close,
        volume,
        prev_close,
        first_close,
        (close - prev_close) / nullif(prev_close, 0) as daily_return
    from with_lag
),

-- Step 2: apply rolling stddev on the already-computed daily_return
final as (
    select
        ticker,
        date,
        close,
        volume,
        daily_return,
        stddev(daily_return) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        ) as volatility_30d,
        (close - first_close) / nullif(first_close, 0) as cumulative_return,
        lag(close, 30) over (partition by ticker order by date) as close_30d_ago
    from with_returns
)

select
    ticker,
    date,
    close,
    volume,
    daily_return,
    volatility_30d,
    cumulative_return,
    (close - close_30d_ago) / nullif(close_30d_ago, 0) as return_30d
from final
