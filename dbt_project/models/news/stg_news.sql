with source as (
    select * from {{ source('raw', 'news') }}
),

cleaned as (
    select
        trim(uuid)                          as uuid,
        upper(trim(ticker))                 as ticker,
        trim(title)                         as title,
        trim(publisher)                     as publisher,
        published_at::timestamp_tz          as published_at,
        trim(link)                          as link,
        ingested_at::timestamp_ntz          as ingested_at,
        row_number() over (
            partition by uuid, ticker
            order by ingested_at desc
        )                                   as row_num
    from source
    where uuid is not null
      and ticker is not null
      and title is not null
)

select
    uuid,
    ticker,
    title,
    publisher,
    published_at,
    link,
    ingested_at
from cleaned
where row_num = 1
