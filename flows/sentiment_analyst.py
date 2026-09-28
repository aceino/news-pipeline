import os
import pandas as pd 
import numpy as np
import pyarrow as pa
from datetime import timedelta
from prefect import flow, task 

from dotenv import load_dotenv

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans

from pyiceberg.types import NestedField, StringType, DoubleType, IntegerType
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema

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
WAREHOUSE = os.getenv("WAREHOUSE")

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

def get_or_create_enriched_table(catalog):
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
            
            # sentiment analyst 
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
 
    enriched_table = get_or_create_enriched_table(catalog)
    write_enriched(enriched_table, enriched_df)
 
    print("ML enrichment complete.")


@flow(name="news-sentiment-analyst", log_prints=True)
def ml_enrichment():
    sentiment()