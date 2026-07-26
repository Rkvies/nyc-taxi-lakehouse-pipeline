import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("test").getOrCreate()

def test_negative_fares_removed(spark):
    data = [(1, -5.0, 2.0), (2, 10.0, 3.0)]
    df = spark.createDataFrame(data, ["id", "fare_amount", "trip_distance"])
    result = df.filter(F.col("fare_amount") > 0)
    assert result.count() == 1
    assert result.collect()[0]["id"] == 2

def test_trip_duration_calculation(spark):
    data = [("2024-01-01 10:00:00", "2024-01-01 10:15:00")]
    df = spark.createDataFrame(data, ["pickup", "dropoff"]).withColumn(
        "pickup", F.to_timestamp("pickup")
    ).withColumn("dropoff", F.to_timestamp("dropoff"))
    result = df.withColumn(
        "duration_min",
        (F.unix_timestamp("dropoff") - F.unix_timestamp("pickup")) / 60
    )
    assert result.collect()[0]["duration_min"] == 15.0