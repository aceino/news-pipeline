
    
    

select
    source_name as unique_field,
    count(*) as n_records

from "news_analytics"."main"."fct_source_performance"
where source_name is not null
group by source_name
having count(*) > 1


