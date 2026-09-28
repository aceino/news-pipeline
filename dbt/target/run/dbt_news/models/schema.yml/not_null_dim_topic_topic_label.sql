
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select topic_label
from "news_analytics"."main"."dim_topic"
where topic_label is null



  
  
      
    ) dbt_internal_test