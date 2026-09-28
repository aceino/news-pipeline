
    

    create  table
      "news_analytics"."main"."fct_sentiment_trend__dbt_tmp"
  
    
    as (
      select
    cast(published_at as date) as published_date,
    round(avg(sentiment_score), 3) as avg_sentiment_score,
    count(*) as article_count
from "news_analytics"."main"."stg_news_sentiment"
group by 1
order by 1
    );
    
  