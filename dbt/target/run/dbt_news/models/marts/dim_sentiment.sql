
    

    create  table
      "news_analytics"."main"."dim_sentiment__dbt_tmp"
  
    
    as (
      with sentiments as ( 
    select distinct
        sentiment_label
    from "news_analytics"."main"."stg_news_sentiment"

    where sentiment_label is not null
)

select 
    row_number() over( 
        order by sentiment_label
    ) as sentiment_key, 

    sentiment_label

from sentiments
    );
    
  