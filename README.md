# Shopping Sessions pipeline

Entry for the Databricks Shopping Sessions track of the Builders Buildathon.

`Dockerfile` builds a small C extension (`pipeline/native/`) and runs `pipeline/pipeline.py`. It reads the CSV logs in `$DATA_DIR`, merges linked user IDs, sessionizes events, attributes sessions to ad clicks and writes `user_daily` and `campaign_daily` Parquet tables to `$OUTPUT_DIR`.

Parsing, deduplication, sessionizing and aggregation run natively across `$NCPU` threads. Polars writes the Parquet output.
