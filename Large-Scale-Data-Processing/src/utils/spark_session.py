"""Utility module for initializing SparkSession with predefined configurations."""
import sys
from pathlib import Path
from pyspark.sql import SparkSession

project_root = str(Path(__file__).parent.parent.parent)
sys.path.append(project_root)
from config.config import SPARK_CONFIG


def get_spark_session(app_name: str = "SparkApp") -> SparkSession:
    """
    Initializes and returns a SparkSession with configurations from config.py.

    Args:
        app_name (str): Name of the Spark application.

    Returns:
        SparkSession: Configured SparkSession object.
    """
    builder = SparkSession.builder
    builder = builder.appName(SPARK_CONFIG.get("spark.app.name", app_name))
    builder = builder.master(SPARK_CONFIG.get("spark.master", "local[*]"))

    for key, value in SPARK_CONFIG.items():
        if key not in ["spark.app.name", "spark.master"]:
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    return spark