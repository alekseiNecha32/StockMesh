-- One row per bank. FITB is the focus; peers provide the comparison baseline.
with returns as (
    select
        ticker,
        return_30d,
        volatility_30d,
        cumulative_return
    from {{ ref('mart_daily_returns') }}
    where date = (select max(date) from {{ ref('mart_daily_returns') }})
),

fundamentals as (
    select
        ticker,
        company_name,
        market_cap,
        trailing_pe,
        price_to_book,
        return_on_equity,
        dividend_yield,
        current_price,
        week_52_position_pct
    from {{ ref('mart_fundamentals') }}
),

sentiment as (
    select
        ticker,
        avg_sentiment,
        news_count,
        bullish_count,
        bearish_count
    from {{ ref('mart_news_sentiment') }}
),

joined as (
    select
        f.ticker,
        f.company_name,
        f.current_price,
        f.market_cap,
        f.trailing_pe,
        f.price_to_book,
        f.return_on_equity,
        f.dividend_yield,
        f.week_52_position_pct,
        r.return_30d,
        r.volatility_30d,
        r.cumulative_return,
        s.avg_sentiment,
        s.news_count,
        s.bullish_count,
        s.bearish_count,
        case when f.ticker = 'FITB' then true else false end as is_focus
    from fundamentals f
    left join returns r   on f.ticker = r.ticker
    left join sentiment s on f.ticker = s.ticker
)

select
    ticker,
    company_name,
    is_focus,
    current_price,
    market_cap,
    trailing_pe,
    price_to_book,
    return_on_equity,
    dividend_yield,
    week_52_position_pct,
    return_30d,
    volatility_30d,
    cumulative_return,
    avg_sentiment,
    news_count,
    bullish_count,
    bearish_count
from joined
order by is_focus desc, ticker
