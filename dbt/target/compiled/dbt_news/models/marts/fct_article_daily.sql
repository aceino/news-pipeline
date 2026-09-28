select
    cast(published_at as date) as published_date,
    count(*) as article_count
from "news_analytics"."main"."stg_news_sentiment"
group by 1
order by 1