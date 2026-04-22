-- Keyword-based sentiment: scores each headline -1 (bearish) to +1 (bullish).
-- Simple but transparent — no external ML dependency needed.
with news as (
    select * from {{ ref('stg_news') }}
),

scored as (
    select
        uuid,
        ticker,
        title,
        publisher,
        published_at,

        case
            when lower(title) like any (
                '%surge%', '%rally%', '%beat%', '%record%', '%upgrade%',
                '%outperform%', '%profit%', '%growth%', '%strong%', '%positive%',
                '%dividend%', '%buyback%', '%bullish%', '%gain%', '%rise%',
                '%raised%', '%exceeded%', '%above%'
            ) then 1
            when lower(title) like any (
                '%crash%', '%plunge%', '%miss%', '%downgrade%', '%loss%',
                '%decline%', '%weak%', '%negative%', '%cut%', '%lower%',
                '%below%', '%concern%', '%risk%', '%lawsuit%', '%probe%',
                '%fell%', '%drop%', '%bearish%', '%layoff%'
            ) then -1
            else 0
        end as sentiment_score

    from news
)

select
    ticker,
    count(*)                            as news_count,
    avg(sentiment_score)                as avg_sentiment,
    sum(case when sentiment_score = 1 then 1 else 0 end)  as bullish_count,
    sum(case when sentiment_score = -1 then 1 else 0 end) as bearish_count,
    sum(case when sentiment_score = 0 then 1 else 0 end)  as neutral_count,
    max(published_at)                   as latest_article_at
from scored
group by ticker
