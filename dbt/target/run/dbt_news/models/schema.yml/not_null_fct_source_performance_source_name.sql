
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_name
from "news_analytics"."main"."fct_source_performance"
where source_name is null



  
  
      
    ) dbt_internal_test