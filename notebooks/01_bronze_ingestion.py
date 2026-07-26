from pyspark.sql import functions as F
from src.utils.config_loader import load_config

cfg = load_config()

# Step 1: Read raw source file (NYC TLC publishes monthly Parquet files)
raw_df = spark.read.parquet(cfg["source_url"])

# Step 2: Add ingestion metadata — required for auditability.
# Every Bronze row must be traceable back to when/where it came from.
bronze_df = (
    raw_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(cfg["source_url"]))
)

# Step 3: Write as Delta table, append mode.
# Bronze is append-only — never overwrite, never delete. This preserves
# full history so you can reprocess Silver logic later without re-fetching source data.
(
    bronze_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(cfg["bronze_table"])
)

print(f"Bronze load complete: {bronze_df.count()} rows written to {cfg['bronze_table']}")