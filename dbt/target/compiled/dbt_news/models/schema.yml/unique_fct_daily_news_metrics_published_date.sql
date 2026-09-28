
    
    

select
    published_date as unique_field,
    count(*) as n_records

from "news_analytics"."main"."fct_daily_news_metrics"
where published_date is not null
group by published_date
having count(*) > 1


