from prefect import flow, task, serve
import os 
import json 
import pandas as pd 
import pyarrow as pa
from datetime import timedelta

from datetime import datetime, timedelta
from newsapi import NewsApiClient
from dotenv import load_dotenv

from kafka import KafkaProducer
from kafka import KafkaConsumer

from pyiceberg.types import NestedField, StringType
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema

from sentiment_analyst import ml_enrichment

load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI")
WAREHOUSE = os.getenv("WAREHOUSE")
N_CLUSTERS = int(os.getenv("ML_N_CLUSTERS", "8")) 

def get_catalog(): 
    return load_catalog(
        "rest",
        type="rest", 
        uri =ICEBERG_REST_URI,
        warehouse=WAREHOUSE,
        ** { 
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
            "s3.region": AWS_REGION,
            "s3.path-style-access": "true",
        }
    )

@task(retries=2, retry_delay_seconds=30, name="producing_news")
def producer_news(): 
    
    newsAPI = NewsApiClient(api_key=NEWS_API_KEY)
    
    producerNews = KafkaProducer ( 
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k : k.encode("utf-8") if k else None,
    ) 

    from_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")

    print(f"[{datetime.now()}] FETCHING TOP HEADLINES FROM NEWSAPI...")

    try : 
        response = newsAPI.get_everything(
            q='jobs',
            language='en',
            from_param=from_date,
            sort_by='relevancy',
            page_size=20
            )

        articles = response.get("articles", [])

        print(f"FETCHED {len(articles)}")

        for article in articles :
            record = { 
                "source": article.get("source", {}).get("name"),
                "author": article.get("author"),
                "title": article.get("title"),
                "description": article.get("description"),
                "url": article.get("url"),
                "published_at": article.get("publishedAt"),
                "content": article.get("content"),
                "fetched_at": datetime.now().isoformat()
            }

            key = article.get("url") or article.get("title")
            producerNews.send(KAFKA_TOPIC, value=record, key=key)
            print(f" SENT {record['title'][:80]} ... ")

        producerNews.flush()
        print(f"[{datetime.now()}] SUCCESS sent {len(articles)} articles to topic {KAFKA_TOPIC}")

    except Exception as e :
        print(f"[{datetime.now()}] ERROR: {e}")
    finally:
        producerNews.close()
        
def get_or_create_table(catalog):
    namespace="news"
    table_name = "news_raw"
    full_name=f"{namespace}.{table_name}"

    try: 
        table = catalog.load_table(full_name)
        print(f"Table {full_name} already exists")

    except Exception: 
        print(f"Creating table{full_name}")

        try :
            catalog.create_namespace(namespace)

        except Exception: 
            pass

        schema  = Schema( 
            NestedField(1, "source", StringType(), required=False),
            NestedField(2, "author", StringType(), required=False),
            NestedField(3, "title", StringType(), required=False),
            NestedField(4, "description", StringType(), required=False),
            NestedField(5, "url", StringType(), required=False),
            NestedField(6, "published_at", StringType(), required=False),
            NestedField(7, "content", StringType(), required=False),
            NestedField(8, "fetched_at", StringType(), required=False),
        )

        table = catalog.create_table( 
            identifier=full_name, 
            schema = schema , 
            location = f"{WAREHOUSE}{namespace}/{table_name}"
        )

        print(f"Table {full_name} created successfully")

    return table 

@task(retries=2, retry_delay_seconds=30, name="consumer_news")
def consumer_news(): 
    print("Starting Consumer")
    print(f"listening to topic {KAFKA_TOPIC}")

    catalog = get_catalog() 
    table = get_or_create_table(catalog)

    consumer = KafkaConsumer ( 

        KAFKA_TOPIC,
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        group_id = KAFKA_GROUP_ID,
        auto_offset_reset = "earliest",
        enable_auto_commit= True,
        consumer_timeout_ms = 15000,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    news = [] 

    for message in consumer : 
        record = message.value 
        print(f"Received {record.get("title", "")[:70]}....")

        news.append(record)

    print("\nStopping Consumer")

    if news : 
        df = pd.DataFrame(news)
        pa_table = pa.Table.from_pandas(df, preserve_index=False)
        table.append(pa_table)

        print(f"Wrote remaining {len(news)} record")

    consumer.close() 
    

@flow(name="news-analytics-pipeline", log_prints=True)
def news_pipeline():
    producer_news()
    consumer_news()

if __name__ =="__main__": 
    ml_enrichment()

if __name__ == "__main__": 
    pipeline_news = news_pipeline.to_deployment(name="news_pipeline_sentiment", interval=timedelta(hours=2))
    sentiment_analyst = ml_enrichment.to_deployment(name="sentiment_analyst", interval=timedelta(hours=1))

    serve(pipeline_news, sentiment_analyst)