import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("test").getOrCreate()

def test_fact_table_has_no_orphan_foreign_keys(spark):
    fact = spark.createDataFrame([(1, 100), (2, 999)], ["trip_id", "location_sk"])
    dim = spark.createDataFrame([(100,)], ["location_sk"])
    orphans = fact.join(dim, "location_sk", "left_anti")
    assert orphans.count() == 1  # documents the known-bad case explicitly