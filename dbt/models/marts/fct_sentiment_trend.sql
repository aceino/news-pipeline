select
    cast(published_at as date) as published_date,
    round(avg(sentiment_score), 3) as avg_sentiment_score,
    count(*) as article_count
from {{ ref('stg_news_sentiment') }}
group by 1
order by 1