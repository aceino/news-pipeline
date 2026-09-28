
    
    

select
    sentiment_key as unique_field,
    count(*) as n_records

from "news_analytics"."main"."dim_sentiment"
where sentiment_key is not null
group by sentiment_key
having count(*) > 1


