from pyspark.sql import functions as F
from src.utils.config_loader import load_config
import datetime

cfg = load_config("../configs/pipeline_config.yaml")
silver_df = spark.table(cfg["silver_table"])

checks = []

# Check 1: no nulls in key columns
null_fare_count = silver_df.filter(F.col("fare_amount").isNull()).count()
checks.append(("null_fare_check", null_fare_count == 0, null_fare_count))

# Check 2: no negative fares slipped through
negative_fare_count = silver_df.filter(F.col("fare_amount") < 0).count()
checks.append(("negative_fare_check", negative_fare_count == 0, negative_fare_count))

# Check 3: row count sanity (not zero, not suspiciously low vs. bronze)
bronze_count = spark.table(cfg["bronze_table"]).count()
silver_count = silver_df.count()
retention_ratio = silver_count / bronze_count
checks.append(("row_retention_check", retention_ratio > 0.8, retention_ratio))

# Log results to a monitoring table (built out in monitoring/pipeline_logging.py in Batch C)
log_rows = [(name, passed, str(detail), datetime.datetime.now()) for name, passed, detail in checks]
log_df = spark.createDataFrame(log_rows, ["check_name", "passed", "detail", "run_at"])

# Ensure the monitoring schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS monitoring")

log_df.write.format("delta").mode("append").saveAsTable("monitoring.dq_check_log")

failed = [c for c in checks if not c[1]]
if failed:
    raise Exception(f"Data quality checks failed: {failed}")

print("All data quality checks passed.")