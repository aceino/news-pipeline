
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select topic_key
from "news_analytics"."main"."dim_topic"
where topic_key is null



  
  
      
    ) dbt_internal_test