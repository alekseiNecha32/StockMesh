with source as (
    select * from {{ source('raw', 'prices') }}
),

cleaned as (
    select
        upper(trim(ticker))         as ticker,
        date::date                  as date,
        open::float                 as open,
        high::float                 as high,
        low::float                  as low,
        close::float                as close,
        volume::bigint              as volume,
        ingested_at::timestamp_ntz  as ingested_at,
        row_number() over (
            partition by ticker, date
            order by ingested_at desc
        )                           as row_num
    from source
    where close is not null
      and date is not null
      and ticker is not null
)

select
    ticker,
    date,
    open,
    high,
    low,
    close,
    volume,
    ingested_at
from cleaned
where row_num = 1
