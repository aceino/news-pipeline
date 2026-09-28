
    

    create  table
      "news_analytics"."main"."fct_source_performance__dbt_tmp"
  
    
    as (
      select
    s.source_name,
    
    count(*) as article_count,

    round(avg(f.sentiment_score), 3) as avg_sentiment_score,

    sum(
        case
            when ds.sentiment_label = 'positive'
            then 1
            else 0
        end
    ) as positive_count,

    sum(
        case
            when ds.sentiment_label = 'negative'
            then 1
            else 0
        end
    ) as negative_count,

    sum(
        case
            when ds.sentiment_label = 'neutral'
            then 1
            else 0
        end
    ) as neutral_count

from "news_analytics"."main"."fact_articles" f

join "news_analytics"."main"."dim_source" s 
    on f.source_key = s.source_key

join "news_analytics"."main"."dim_sentiment" ds
    on f.sentiment_key = ds.sentiment_key

group by s.source_name

order by article_count desc
    );
    
  