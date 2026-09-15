from prefect import flow, task 
import os 
import json 
import pandas as pd 
import numpy as np
import pyarrow as pa
from datetime import timedelta

from datetime import datetime, timedelta
from newsapi import NewsApiClient
from dotenv import load_dotenv

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans

from kafka import KafkaProducer
from kafka import KafkaConsumer

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType, IntegerType

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
RAW_NAMESPACE = "news"
RAW_TABLE = "news_raw"
ENRICHED_NAMESPACE = "news"
ENRICHED_TABLE = "news_sentiment"

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
            q='china',
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

def load_raw_article(catalog) -> pd.DataFrame: 
    full_name = f"{RAW_NAMESPACE}.{RAW_TABLE}"
    table = catalog.load_table(full_name)
    df = table.scan().to_pandas()
    print(f"Loaded {len(df)} rows from {full_name}")

    before = len(df)
    df = df.sort_values("fetched_at", ascending=False).drop_duplicates(subset="url", keep="first")
    print(f"Deduplicated by url: {before} -> {len(df)} rows")

    return df

def sentiment_analyst(df: pd.DataFrame, n_clusters: int=N_CLUSTERS) -> pd.DataFrame :
    if df.empty:
        print("No Rows to enrich")
        return df 
    
    df = df.copy()
    df["text_for_analyst"] = (
        df["title"].fillna("") + ". " + df["description"].fillna("")
    )

    analyzer = SentimentIntensityAnalyzer()
    scores = df["text_for_analyst"].apply(analyzer.polarity_scores)
    df["sentiment_score"] = scores.apply(lambda s :s["compound"]).astype(float)

    def sentiment_score(score: float)-> str : 
        if score >= 0.05:
            return "positive"
        elif score <= -0.05:
            return "negative"
        return "neutral"
    
    df["sentiment_label"] = df["sentiment_score"].apply(sentiment_score)

    vectorizer = TfidfVectorizer ( 
        max_features=1000,
        stop_words="english",
        ngram_range=(1, 2),
        min_df=1,
    )

    tfdfi_matrix = vectorizer.fit_transform(df["text_for_analyst"])

    k=min(n_clusters, len(df))
    kmeans =KMeans(n_clusters=k, random_state=42, n_init=10)
    df["topic_cluster"] = kmeans.fit_predict(tfdfi_matrix).astype("int32")

    terms = np.array(vectorizer.get_feature_names_out())
    order_centroids = kmeans.cluster_centers_.argsort()[:, ::-1]
    cluster_labels = { 
        i: ", ".join(terms[order_centroids[i, :3]]) for i in range(k)
    }

    df["topic_label"] = df["topic_cluster"].map(cluster_labels)
 
    return df.drop(columns=["text_for_analyst"])

def get_or_create_enriched_table(catalog, sample_df: pd.DataFrame):
    full_name = f"{ENRICHED_NAMESPACE}.{ENRICHED_TABLE}"
 
    try:
        table = catalog.load_table(full_name)
        print(f"Table {full_name} already exists.")
        return table
    except Exception:
        print(f"Creating table {full_name}")
 
        try:
            catalog.create_namespace(ENRICHED_NAMESPACE)
        except Exception:
            pass
 
        schema = Schema(
            NestedField(1, "source", StringType(), required=False),
            NestedField(2, "author", StringType(), required=False),
            NestedField(3, "title", StringType(), required=False),
            NestedField(4, "description", StringType(), required=False),
            NestedField(5, "url", StringType(), required=False),
            NestedField(6, "published_at", StringType(), required=False),
            NestedField(7, "content", StringType(), required=False),
            NestedField(8, "fetched_at", StringType(), required=False),
            NestedField(9, "sentiment_score", DoubleType(), required=False),
            NestedField(10, "sentiment_label", StringType(), required=False),
            NestedField(11, "topic_cluster", IntegerType(), required=False),
            NestedField(12, "topic_label", StringType(), required=False),
        )
 
        table = catalog.create_table(
            identifier=full_name,
            schema=schema,
            location=f"{WAREHOUSE}{ENRICHED_NAMESPACE}/{ENRICHED_TABLE}",
        )
        print(f"Table {full_name} created successfully")
        return table
    
def write_enriched(table, df: pd.DataFrame):
    if df.empty:
        print("Nothing to write.")
        return
 
    # Column order must match the Iceberg schema field order
    ordered_cols = [
        "source", "author", "title", "description", "url",
        "published_at", "content", "fetched_at",
        "sentiment_score", "sentiment_label", "topic_cluster", "topic_label",
    ]
    df = df[ordered_cols]
 
    pa_table = pa.Table.from_pandas(df, preserve_index=False)
 
    # Full overwrite: keeps clustering consistent across runs (see module
    # docstring). Swap to table.append(pa_table) later if you move to a
    # true incremental design.
    table.overwrite(pa_table)
    print(f"Wrote {len(df)} enriched rows.")

@task(retries=1, retry_delay_seconds=15, name="sentiment_anlyst")
def sentiment() :
    print("Starting ML enrichment step")
    catalog = get_catalog()
 
    raw_df = load_raw_article(catalog)
    enriched_df = sentiment_analyst(raw_df)
 
    enriched_table = get_or_create_enriched_table(catalog, enriched_df)
    write_enriched(enriched_table, enriched_df)
 
    print("ML enrichment complete.")

@flow(name="news-analytics-pipeline", log_prints=True)
def news_pipeline():
    producer_news()
    consumer_news()
    sentiment()

if __name__ == "__main__": 
    news_pipeline.serve(name="news_pipeline_sentiment", interval=timedelta(hours=2))