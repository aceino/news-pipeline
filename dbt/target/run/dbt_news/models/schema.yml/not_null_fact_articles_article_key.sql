
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select article_key
from "news_analytics"."main"."fact_articles"
where article_key is null



  
  
      
    ) dbt_internal_test