import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from pyspark.sql.functions import (col, count, hash, isnan, kurtosis, lit, max,
                                 mean, min, skewness, stddev, when)

project_root = str(Path(__file__).parent.parent.parent)
sys.path.append(project_root)

from config.config import EXPECTED_SCHEMA, RAW_DATA_PATH, REPORTS_PATH
from src.utils.spark_session import get_spark_session


def generate_data_quality_report(spark, df_path, stage, report_path, expected_schema):
    """
    Generates an HTML formatted data quality report for the given DataFrame.

    Args:
        spark: A SparkSession object.
        df: The PySpark DataFrame to analyze.
        stage (str): The stage of the data pipeline.
        report_path (str): The directory to save the report.
        expected_schema: Schema definition for the data.
    """
    df = spark.read.csv(df_path, header=True, inferSchema=True)
    num_rows = df.count()
    num_cols = len(df.columns)

    missing_counts = df.select(
        [count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]
    )
    missing_df = missing_counts.toPandas().transpose()
    missing_df.columns = ["Missing Count"]
    missing_percent = (missing_df["Missing Count"] / num_rows) * 100
    missing_df["Missing Percent"] = missing_percent
    missing_df = missing_df.applymap(lambda x: f"{x:.2f}")  # Format for HTML

    numeric_cols = [c for c in df.columns if c not in ["Time", "Class"]]

    stats_dfs = []

    for stat_func in [mean, stddev, min, max, skewness, kurtosis]:
        stats_row = [lit(stat_func.__name__).alias("stats_name")]  # Use function name
        for c in numeric_cols:
            stats_row.append(stat_func(c).alias(c))
        stats_dfs.append(df.select(stats_row).toPandas())

    summary_stats = pd.concat(stats_dfs, ignore_index=True)
    summary_stats = summary_stats.apply(
        lambda x: x.map(
            lambda y: f"{float(y):.2f}" if isinstance(y, (int, float)) else y
        )
    )

    schema_match = df.schema == expected_schema
    schema_check_message = (
        "Schema matches expected schema."
        if schema_match
        else "Schema does NOT match expected schema."
    )

    if not schema_match:
        current_schema_dict = {f.name: f.dataType for f in df.schema.fields}
        expected_schema_dict = {f.name: f.dataType for f in expected_schema.fields}

        differences = []
        current_cols = set(current_schema_dict.keys())
        expected_cols = set(expected_schema_dict.keys())

        missing_cols = expected_cols - current_cols
        extra_cols = current_cols - expected_cols

        if missing_cols:
            differences.append(f"Missing columns: {', '.join(missing_cols)}")
        if extra_cols:
            differences.append(f"Extra columns: {', '.join(extra_cols)}")

        for column in current_cols.intersection(expected_cols):
            if current_schema_dict[column] != expected_schema_dict[column]:
                differences.append(
                    f"Column '{column}' has type {current_schema_dict[column]} "
                    f"but expected {expected_schema_dict[column]}"
                )

        schema_check_message += "<br><br>Differences found:<br>" + "<br>".join(
            differences
        )

    df = df.withColumn("row_hash", hash(*df.columns))

    duplicate_counts = df.groupBy("row_hash").count().filter(col("count") > 1)

    num_duplicates = duplicate_counts.count()
    duplicate_check_message = f"Found {num_duplicates} duplicate rows."

    duplicate_samples = None
    if num_duplicates > 0:
        duplicate_samples = (
            duplicate_counts.join(df, "row_hash")
            .select(df.columns)
            .orderBy("row_hash")
            .limit(10)
            .toPandas()
        )
        duplicate_check_message += "<br><br>Sample of duplicate rows:<br>"
        duplicate_check_message += duplicate_samples.to_html(index=False)

    # Create HTML Report Content
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Data Quality Report ({stage})</title>
    </head>
    <body>
        <h1>Data Quality Report ({stage})</h1>
        <p>Timestamp: {timestamp}</p>

        <h2>Basic Information</h2>
        <p>Number of Rows: {num_rows}</p>
        <p>Number of Columns: {num_cols}</p>

        <h2>Missing Values</h2>
        {missing_df.to_html(index=True)}

        <h2>Summary Statistics</h2>
        {summary_stats.to_html(index=False)}

        <h2>Schema Check</h2>
        <p>{schema_check_message}</p>

        <h2>Duplicate Check</h2>
        <p>{duplicate_check_message}</p>

    </body>
    </html>
    """

    report_filename = (
        f"data_quality_report_{stage}_{timestamp.replace(' ', '_').replace(':', '')}.html"
    )
    report_filepath = os.path.join(report_path, report_filename)

    try:
        report_df = spark.createDataFrame([(html_content,)], ["Report Content"])
        report_df.coalesce(1).write.text(report_filepath, mode="overwrite")
        print(f"Data quality report saved to: {report_filepath}")

    except Exception as e:
        print(f"Error writing report with Spark: {e}")
        print("Falling back to writing report with Python.")

        with open(report_filepath, "w") as f:
            f.write(html_content)

        print(f"Data quality report saved to: {report_filepath}")