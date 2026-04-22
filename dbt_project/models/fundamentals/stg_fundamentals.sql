with source as (
    select * from {{ source('raw', 'fundamentals') }}
),

cleaned as (
    select
        upper(trim(ticker))         as ticker,
        market_cap::float           as market_cap,
        trailing_p_e::float         as trailing_pe,
        forward_p_e::float          as forward_pe,
        price_to_book::float        as price_to_book,
        trailing_eps::float         as trailing_eps,
        forward_eps::float          as forward_eps,
        dividend_yield::float       as dividend_yield,
        payout_ratio::float         as payout_ratio,
        return_on_equity::float     as return_on_equity,
        return_on_assets::float     as return_on_assets,
        debt_to_equity::float       as debt_to_equity,
        current_ratio::float        as current_ratio,
        trim(sector)                as sector,
        trim(industry)              as industry,
        full_time_employees::bigint as full_time_employees,
        trim(long_name)             as company_name,
        current_price::float        as current_price,
        fifty_two_week_high::float  as week_52_high,
        fifty_two_week_low::float   as week_52_low,
        ingested_at::timestamp_ntz  as ingested_at,
        row_number() over (
            partition by ticker
            order by ingested_at desc
        )                           as row_num
    from source
    where ticker is not null
)

select
    ticker,
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
    sector,
    industry,
    full_time_employees,
    company_name,
    current_price,
    week_52_high,
    week_52_low,
    ingested_at
from cleaned
where row_num = 1
