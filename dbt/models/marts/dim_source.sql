with sources as ( 
    select distinct 
        source as source_name 
    from {{ ref('stg_news_sentiment') }}

    where source is not null
)


select 
    row_number() over (order by source_name) as source_key, 
    source_name 

from sources 