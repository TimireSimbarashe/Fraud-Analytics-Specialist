"""Module for preparing credit card fraud data for analysis."""

from pyspark.sql.functions import col
from pyspark.sql.types import StructType


def ingest_data(spark, raw_data_path: str, expected_schema: StructType):
    """Ingest the credit card fraud dataset from the raw data directory.

    Args:
        spark: A SparkSession object.
        raw_data_path: Path to the raw data file.
        expected_schema: Schema definition for the data.

    Returns:
        A PySpark DataFrame containing the raw data.
    """
    df = spark.read.csv(
        raw_data_path,
        header=True,
        schema=expected_schema,
        inferSchema=False,
    )
    return df


def clean_data(df, processed_data_path: str):
    """Prepare the data for analysis.

    Deduplicates rows, handles missing values, and converts column types.

    Args:
        df: The raw PySpark DataFrame.
        processed_data_path: Path to save the processed data.

    Returns:
        A PySpark DataFrame with deduplicated data and proper column types.
    """
    # Handle duplicate rows
    df = df.dropDuplicates()

    # Handle missing values by imputing with mean for numeric columns
    numeric_cols = [c for c in df.columns if c not in ["Time", "Class"]]
    for col_name in numeric_cols:
        mean_val = df.select(col(col_name)).agg({col_name: "mean"}).collect()[0][0]
        df = df.fillna(mean_val, subset=[col_name])

    # Convert Time and Class columns to integers
    df = df.withColumn("Time", col("Time").cast("integer"))
    df = df.withColumn("Class", col("Class").cast("integer"))

    return df.write.csv(processed_data_path, header=True, mode="overwrite")
