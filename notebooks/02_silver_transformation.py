from pyspark.sql import functions as F
from src.utils.config_loader import load_config

cfg = load_config("../configs/pipeline_config.yaml")
bronze_df = spark.table(cfg["bronze_table"])

# Step 1: Deduplicate.
# NYC TLC data can contain duplicate rows across overlapping monthly extracts.
deduped_df = bronze_df.dropDuplicates([
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID"
])

# Step 2: Filter out impossible / bad records.
# Document every rule — this becomes your docs/decisions.md content.
clean_df = (
    deduped_df
    .filter(F.col("fare_amount") > 0)                     # negative/zero fares are data errors
    .filter(F.col("trip_distance") > 0)                    # zero-distance trips are not real trips
    .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))  # dropoff must be after pickup
    .filter(F.col("passenger_count").between(1, 6))         # sane passenger range
)

# Step 3: Standardize types and derive fields used downstream.
silver_df = (
    clean_df
    .withColumn("trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60)
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("_processed_at", F.current_timestamp())
)

# Step 4: Write to Silver as a MERGE (upsert), not a blind overwrite.
# This makes the pipeline idempotent — rerunning it for the same source
# data doesn't create duplicates. Idempotency is a core production requirement.
from delta.tables import DeltaTable

# Ensure the target schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

if spark.catalog.tableExists(cfg["silver_table"]):
    silver_table = DeltaTable.forName(spark, cfg["silver_table"])
    (
        silver_table.alias("t")
        .merge(
            silver_df.alias("s"),
            "t.VendorID = s.VendorID AND t.tpep_pickup_datetime = s.tpep_pickup_datetime "
            "AND t.PULocationID = s.PULocationID"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver_df.write.format("delta").saveAsTable(cfg["silver_table"])

print(f"Silver load complete: {silver_df.count()} rows processed")