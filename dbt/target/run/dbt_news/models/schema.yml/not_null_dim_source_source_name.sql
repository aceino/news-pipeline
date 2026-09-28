
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_name
from "news_analytics"."main"."dim_source"
where source_name is null



  
  
      
    ) dbt_internal_test