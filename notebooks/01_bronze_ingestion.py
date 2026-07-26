# notebooks/01_bronze_ingestion.py
# Updated: uses Unity Catalog Volumes for the landing zone instead of DBFS
# (DBFS root is restricted in Databricks Free Edition workspaces)

import os
import urllib.request
from pyspark.sql import functions as F
from src.utils.config_loader import load_config

cfg = load_config("../configs/pipeline_config.yaml")

# ---------------------------------------------------------------------------
# STEP 0: Ensure catalog/schema/volume exist.
# Free Edition gives you a default catalog (usually "workspace") — adjust
# catalog_name below if yours differs (check Catalog icon in left sidebar).
# ---------------------------------------------------------------------------
catalog_name = "workspace"
landing_schema = "landing"
volume_name = "raw_files"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{landing_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{landing_schema}.{volume_name}")

# Volumes are FUSE-mounted at this path — usable as a normal local path, no dbfs: prefix needed
volume_path = f"/Volumes/{catalog_name}/{landing_schema}/{volume_name}/"

# ---------------------------------------------------------------------------
# STEP 1: Landing Zone — download raw source file into the Volume, untouched.
# ---------------------------------------------------------------------------
source_url = cfg["source_url"]
file_name = source_url.split("/")[-1]                 # e.g. yellow_tripdata_2024-01.parquet
local_landing_path = f"{volume_path}{file_name}"        # this IS a real local-fs path under Volumes

if not os.path.exists(local_landing_path):
    print(f"Downloading {source_url} ...")
    urllib.request.urlretrieve(source_url, local_landing_path)
    print(f"Downloaded to {local_landing_path}")
else:
    print(f"File already present in landing zone, skipping download: {local_landing_path}")

# ---------------------------------------------------------------------------
# STEP 2: Bronze — read the landed file and write to Delta with ingestion metadata.
# ---------------------------------------------------------------------------
raw_df = spark.read.parquet(local_landing_path)

bronze_df = (
    raw_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(file_name))
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze")

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{catalog_name}.bronze.trips_raw")
)

row_count = bronze_df.count()
print(f"Bronze load complete: {row_count} rows written to {catalog_name}.bronze.trips_raw")