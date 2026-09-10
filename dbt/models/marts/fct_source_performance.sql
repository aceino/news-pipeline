select
    source,
    count(*) as article_count,
    round(avg(sentiment_score), 3) as avg_sentiment_score,
    sum(case when sentiment_label = 'positive' then 1 else 0 end) as positive_count,
    sum(case when sentiment_label = 'negative' then 1 else 0 end) as negative_count,
    sum(case when sentiment_label = 'neutral' then 1 else 0 end) as neutral_count
from {{ ref('stg_news_sentiment') }}
group by 1
order by article_count desc