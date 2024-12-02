"""Main script to run the credit card fraud detection pipeline."""

import sys
from pathlib import Path

project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from config.config import (
    RAW_DATA_PATH,
    PROCESSED_DATA_PATH,
    TRANSFORMED_DATA_PATH,
    KAGGLE_DATASET,
    KAGGLE_FILENAME,
    EXPECTED_SCHEMA,
    REPORTS_PATH
)
from src.pipeline import run_pipeline
from src.utils.spark_session import get_spark_session


if __name__ == "__main__":
    spark = get_spark_session()
    run_pipeline(
        spark,
        KAGGLE_DATASET,
        KAGGLE_FILENAME,
        RAW_DATA_PATH,
        EXPECTED_SCHEMA,
        PROCESSED_DATA_PATH,
        TRANSFORMED_DATA_PATH,
        REPORTS_PATH
    )
