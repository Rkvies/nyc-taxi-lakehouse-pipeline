from pyspark.sql import DataFrame, functions as F
from dataclasses import dataclass
from typing import Callable
import datetime

@dataclass
class DQCheck:
    name: str
    check_fn: Callable[[DataFrame], bool]
    detail_fn: Callable[[DataFrame], str]

def null_check(column: str) -> DQCheck:
    return DQCheck(
        name=f"null_check_{column}",
        check_fn=lambda df: df.filter(F.col(column).isNull()).count() == 0,
        detail_fn=lambda df: str(df.filter(F.col(column).isNull()).count())
    )

def range_check(column: str, min_val, max_val) -> DQCheck:
    return DQCheck(
        name=f"range_check_{column}",
        check_fn=lambda df: df.filter(~F.col(column).between(min_val, max_val)).count() == 0,
        detail_fn=lambda df: str(df.filter(~F.col(column).between(min_val, max_val)).count())
    )

def freshness_check(column: str, max_age_hours: int) -> DQCheck:
    return DQCheck(
        name=f"freshness_check_{column}",
        check_fn=lambda df: df.agg(F.max(column)).collect()[0][0] is not None and
            (datetime.datetime.now() - df.agg(F.max(column)).collect()[0][0]).total_seconds() / 3600 <= max_age_hours,
        detail_fn=lambda df: str(df.agg(F.max(column)).collect()[0][0])
    )

def run_checks(df: DataFrame, checks: list[DQCheck], spark, log_table: str) -> None:
    results = []
    for c in checks:
        passed = c.check_fn(df)
        detail = c.detail_fn(df)
        results.append((c.name, passed, detail, datetime.datetime.now()))

    log_df = spark.createDataFrame(results, ["check_name", "passed", "detail", "run_at"])
    log_df.write.format("delta").mode("append").saveAsTable(log_table)

    failed = [r for r in results if not r[1]]
    if failed:
        raise Exception(f"Data quality checks failed: {[f[0] for f in failed]}")