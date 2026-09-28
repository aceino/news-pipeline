
    
    

select
    topic_key as unique_field,
    count(*) as n_records

from "news_analytics"."main"."dim_topic"
where topic_key is not null
group by topic_key
having count(*) > 1


