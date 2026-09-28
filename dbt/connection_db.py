import duckdb

con = duckdb.connect()

con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("INSTALL iceberg; LOAD iceberg;")

# S3 credentials for reading the underlying Parquet files in MinIO
con.sql("""
    CREATE SECRET minio_secret (
        TYPE s3,
        KEY_ID 'admin',
        SECRET 'password',
        ENDPOINT 'localhost:9000',
        URL_STYLE 'path',
        USE_SSL false
    );
""")

# Attach the Iceberg REST catalog itself
con.sql("""
    ATTACH '' AS iceberg_catalog (
        TYPE ICEBERG,
        ENDPOINT 'http://localhost:8181',
        AUTHORIZATION_TYPE 'none',
        ACCESS_DELEGATION_MODE 'none'
    );
""")

# Now query it
result = con.sql("SELECT * FROM iceberg_catalog.news.news_sentiment LIMIT 5;")
print(result)