select
        t.topic_cluster,
        t.topic_label,

        count(*) as article_count,

        round(avg(sentiment_score), 3) as avg_sentiment_score

    from "news_analytics"."main"."fact_articles" f 

    join "news_analytics"."main"."dim_topic" t 
        on f.topic_key = t.topic_key

    group by 1,2 

    order by article_count desc