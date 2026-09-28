with articles as (

    select
        url,
        source,
        topic_cluster,
        topic_label,
        sentiment_label,
        published_at,
        fetched_at,
        sentiment_score

    from {{ ref('stg_news_sentiment') }}

),

final as (

    select
        row_number() over (
            order by articles.url
        ) as article_key,   

        date_dim.date_key,
        source_dim.source_key,
        topic_dim.topic_key,
        sentiment_dim.sentiment_key,

        articles.published_at,
        articles.fetched_at,
        articles.sentiment_score

    from articles

    left join {{ ref('dim_date') }} as date_dim
        on cast(articles.published_at as date) = date_dim.full_date

    left join {{ ref('dim_source') }} as source_dim
        on articles.source = source_dim.source_name

    left join {{ ref('dim_topic') }} as topic_dim
        on articles.topic_cluster = topic_dim.topic_cluster
        and articles.topic_label = topic_dim.topic_label

    left join {{ ref('dim_sentiment') }} as sentiment_dim
        on articles.sentiment_label = sentiment_dim.sentiment_label

)

select *
from final