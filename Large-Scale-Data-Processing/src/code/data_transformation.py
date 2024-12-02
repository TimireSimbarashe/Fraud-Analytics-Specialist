"""Module for transforming credit card fraud data."""

from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    dayofweek,
    hour,
    kurtosis,
    lag,
    lead,
    log1p,
    max,
    mean,
    min,
    skewness,
    stddev,
    sum,
    when,
)
from pyspark.sql.types import StructType
from pyspark.sql.window import Window


def transform_data(
    spark: SparkSession,
    processed_data_path: str,
    transformed_data_path: str,
    expected_schema: StructType,
):
    """Transform the preprocessed data by adding new features.

    Args:
        spark: SparkSession object for Spark operations
        processed_data_path: Path to the preprocessed data
        transformed_data_path: Path to save transformed data
        expected_schema: Schema definition for the data

    Returns:
        A PySpark DataFrame with transformed data.
    """
    df = spark.read.csv(processed_data_path, header=True, schema=expected_schema)

    df = df.withColumn("HourOfDay", hour((col("Time") / 86400).cast("timestamp")))
    df = df.withColumn("DayOfWeek", dayofweek((col("Time") / 86400).cast("timestamp")))

    window_spec = Window.partitionBy("Class").orderBy("Time")
    df = df.withColumn(
        "TimeSincePrevTransaction", col("Time") - lag(col("Time"), 1).over(window_spec)
    )
    df = df.withColumn(
        "TimeToNextTransaction", lead(col("Time"), 1).over(window_spec) - col("Time")
    )

    df = df.fillna({"TimeSincePrevTransaction": 0, "TimeToNextTransaction": 0})

    window_spec_3 = Window.partitionBy("Class").orderBy("Time").rowsBetween(-2, 0)
    window_spec_7 = Window.partitionBy("Class").orderBy("Time").rowsBetween(-6, 0)

    df = df.withColumn("Amount_mean_3", mean("Amount").over(window_spec_3))
    df = df.withColumn("Amount_std_3", stddev("Amount").over(window_spec_3))
    df = df.withColumn("Amount_mean_7", mean("Amount").over(window_spec_7))
    df = df.withColumn("Amount_std_7", stddev("Amount").over(window_spec_7))

    df = df.fillna(
        {
            "Amount_mean_3": 0,
            "Amount_std_3": 0,
            "Amount_mean_7": 0,
            "Amount_std_7": 0,
        }
    )

    df = df.withColumn("LogAmount", log1p(col("Amount")))

    for w in [3, 7]:
        df = df.withColumn(
            f"Amount_zscore_{w}",
            (col("Amount") - col(f"Amount_mean_{w}")) / col(f"Amount_std_{w}"),
        )
        df = df.withColumn(
            f"Amount_diff_mean_{w}",
            col("Amount") - col(f"Amount_mean_{w}"),
        )
        df = df.withColumn(
            f"Amount_ratio_mean_{w}",
            col("Amount") / col(f"Amount_mean_{w}"),
        )

    df = df.fillna(
        {
            "Amount_zscore_3": 0,
            "Amount_zscore_7": 0,
            "Amount_diff_mean_3": 0,
            "Amount_diff_mean_7": 0,
            "Amount_ratio_mean_3": 0,
            "Amount_ratio_mean_7": 0,
        }
    )

    df = df.withColumn("Amount_skewness", skewness("Amount").over(window_spec_3))
    df = df.withColumn("Amount_kurtosis", kurtosis("Amount").over(window_spec_3))

    df = df.fillna(
        {
            "Amount_skewness": 0,
            "Amount_kurtosis": 0,
        }
    )

    aggregation_window = Window.partitionBy("Class")
    df = df.withColumn("Total_Transactions", count("Time").over(aggregation_window))
    df = df.withColumn("Total_Amount", sum("Amount").over(aggregation_window))
    df = df.withColumn("Max_Transaction_Amount", max("Amount").over(aggregation_window))
    df = df.withColumn("Min_Transaction_Amount", min("Amount").over(aggregation_window))

    df = df.withColumn(
        "IsLargeTransaction",
        when(col("Amount") > col("Amount_mean_7") + 3 * col("Amount_std_7"), 1).otherwise(
            0
        ),
    )

    return df.coalesce(1).write.csv(transformed_data_path, header=True, mode="overwrite")
