select
    topic_cluster,
    topic_label,
    count(*) as mention_count,
    round(avg(sentiment_score), 3) as avg_sentiment_score
from {{ ref('stg_news_sentiment') }}
group by 1, 2
order by mention_count desc