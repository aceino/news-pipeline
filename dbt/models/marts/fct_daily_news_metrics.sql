select
    d.full_date as published_date,
    
    count(*) as article_count,

    count(distinct f.source_key) as unique_source,

    count(distinct f.topic_key) as unique_topics,

    round(avg(f.sentiment_score), 3) as avg_sentiment_score
    
from {{ ref('fact_articles') }} as f 

join {{ ref('dim_date') }} d 

    on f.date_key = d.date_key

group by    
    d.full_date

order by
    d.full_date