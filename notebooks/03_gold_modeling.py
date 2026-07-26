from pyspark.sql import functions as F
from src.utils.config_loader import load_config

cfg = load_config("../configs/pipeline_config.yaml")
silver_df = spark.table(cfg["silver_table"])

# Ensure the target schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# --- Dimension: Date ---
dim_date_df = (
    silver_df.select(F.col("pickup_date").alias("date_key")).distinct()
    .withColumn("year", F.year("date_key"))
    .withColumn("month", F.month("date_key"))
    .withColumn("day_of_week", F.dayofweek("date_key"))
    .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
)
dim_date_df.write.format("delta").mode("overwrite").saveAsTable(cfg["gold_dim_date"])

# --- Dimension: Location ---
# (In a real build, join against the TLC zone lookup CSV for borough/zone names —
#  omitted here for brevity, but note it explicitly in your notebook as a TODO/step.)
dim_location_df = (
    silver_df.select(F.col("PULocationID").alias("location_id")).distinct()
    .withColumn("location_sk", F.monotonically_increasing_id())  # surrogate key
)
dim_location_df.write.format("delta").mode("overwrite").saveAsTable(cfg["gold_dim_location"])

# --- Dimension: Vendor ---
dim_vendor_df = (
    silver_df.select(F.col("VendorID").alias("vendor_id")).distinct()
    .withColumn("vendor_sk", F.monotonically_increasing_id())
)
dim_vendor_df.write.format("delta").mode("overwrite").saveAsTable(cfg["gold_dim_vendor"])

# --- Fact: Trips ---
fact_trips_df = (
    silver_df
    .join(dim_location_df, silver_df.PULocationID == dim_location_df.location_id, "left")
    .join(dim_vendor_df, silver_df.VendorID == dim_vendor_df.vendor_id, "left")
    .select(
        "pickup_date", "location_sk", "vendor_sk",
        "trip_distance", "fare_amount", "tip_amount",
        "trip_duration_minutes", "passenger_count"
    )
)

(
    fact_trips_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("pickup_date")     # partition fact table by date — matches typical query patterns
    .saveAsTable(cfg["gold_fact_table"])
)

print("Gold star schema build complete.")