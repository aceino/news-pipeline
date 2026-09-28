with topics as ( 
    select distinct
        topic_cluster, 
        topic_label
    from "news_analytics"."main"."stg_news_sentiment"

    where topic_label is not null
)

select 
    row_number() over( 
        order by topic_cluster, topic_label
    ) as topic_key, 

    topic_cluster, 
    topic_label

from topics