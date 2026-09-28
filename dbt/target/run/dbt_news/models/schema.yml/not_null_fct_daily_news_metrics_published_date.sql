
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select published_date
from "news_analytics"."main"."fct_daily_news_metrics"
where published_date is null



  
  
      
    ) dbt_internal_test