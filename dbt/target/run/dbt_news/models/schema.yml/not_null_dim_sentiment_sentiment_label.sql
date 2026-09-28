
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sentiment_label
from "news_analytics"."main"."dim_sentiment"
where sentiment_label is null



  
  
      
    ) dbt_internal_test