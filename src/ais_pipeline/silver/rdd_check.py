"""RDD map/reduce cross-check of the exact-duplicate count.

This is the one place the pipeline uses the low-level RDD API, on purpose: it recounts exact
duplicates with a classic map -> reduceByKey -> filter -> map -> sum chain, independent of the
DataFrame code path, and the two numbers must agree. The RDD version is slower because every
row is serialised to Python and Catalyst cannot optimise lambdas; the timing comparison is in
docs/performance.md.
"""

from __future__ import annotations

from operator import add

from pyspark.sql import DataFrame

from ais_pipeline.silver.standardize import SOURCE_COLS


def exact_duplicates_rdd(bronze_df: DataFrame, cols: list[str] | None = None) -> int:
    """Rows that are an extra copy of another row (all source columns equal), via RDDs."""
    cols = cols or SOURCE_COLS
    return (
        bronze_df.select(*cols)
        .rdd.map(lambda row: (tuple(row), 1))  # key: the whole row
        .reduceByKey(add)  # copies per distinct row (combines map-side first)
        .filter(lambda kv: kv[1] > 1)  # only rows seen more than once
        .map(lambda kv: kv[1] - 1)  # extra copies beyond the first
        .sum()
    )
