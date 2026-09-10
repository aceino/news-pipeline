select
    source,
    author,
    title,
    description,
    url,
    cast(published_at as timestamp) as published_at,
    cast(fetched_at as timestamp) as fetched_at,
    sentiment_score,
    sentiment_label,
    topic_cluster,
    topic_label
from {{ source('iceberg_catalog', 'news_sentiment') }}