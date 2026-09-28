
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_key
from "news_analytics"."main"."fact_articles"
where source_key is null



  
  
      
    ) dbt_internal_test