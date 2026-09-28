import duckdb

con = duckdb.connect("news_analytics.duckdb")

con.execute("CALL start_ui()")

input("DuckDB UI is running. Press Enter to stop...")