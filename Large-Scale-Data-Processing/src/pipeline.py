"""Pipeline module for orchestrating the credit card fraud detection workflow."""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from .code.data_extraction import download_dataset_from_kaggle
from .code.data_preparation import ingest_data, clean_data
from .code.data_transformation import transform_data
from .code.data_quality_checks import generate_data_quality_report

def run_pipeline(
    spark: SparkSession,
    kaggle_dataset: str,
    kaggle_filename: str,
    raw_data_path: str,
    expected_schema: StructType,
    processed_data_path: str,
    transformed_data_path: str,
    reports_path: str,
):
    """Execute the complete data processing pipeline."""
    logging.info("Downloading dataset from Kaggle...")
    download_dataset_from_kaggle(raw_data_path, kaggle_dataset, kaggle_filename)

    logging.info("Ingesting data...")
    raw_df = ingest_data(spark, raw_data_path, expected_schema)
    generate_data_quality_report(
        spark, raw_data_path, "raw", reports_path, expected_schema
    )

    logging.info("Cleaning data...")
    clean_data(raw_df, processed_data_path)
    generate_data_quality_report(
        spark, processed_data_path, "prepared", reports_path, expected_schema
    )

    logging.info("Transforming data...")
    transform_data(
        spark, processed_data_path, transformed_data_path, expected_schema
    )
    generate_data_quality_report(
        spark, transformed_data_path, "transformed", reports_path, expected_schema
    )

    logging.info("Pipeline completed successfully.")
    spark.stop()
    
    
