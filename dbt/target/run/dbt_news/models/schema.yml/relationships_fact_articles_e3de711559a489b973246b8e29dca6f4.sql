
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select sentiment_key as from_field
    from "news_analytics"."main"."fact_articles"
    where sentiment_key is not null
),

parent as (
    select sentiment_key as to_field
    from "news_analytics"."main"."dim_sentiment"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test