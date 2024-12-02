"""Data transformation module for cleaning and processing raw data."""

import logging
from pathlib import Path
import sys

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_root = str(Path(__file__).parent.parent.parent)
sys.path.append(project_root)

from config.config import PROCESSED_DATA_PATH, START_DATE


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values in the dataframe by dropping rows with null values.

    Args:
        df: Input DataFrame to process

    Returns:
        pd.DataFrame: DataFrame with missing values removed
    """
    missing_values = df.isnull().sum()
    if missing_values.any():
        logger.warning(f"Missing values found:\n{missing_values}")
        df = df.dropna()
        logger.info("Dropped rows with missing values.")
    return df


def handle_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from the dataframe.

    Args:
        df: Input DataFrame to process

    Returns:
        pd.DataFrame: DataFrame with duplicate rows removed
    """
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        logger.warning(f"Found {duplicates} duplicate rows. Removing duplicates...")
        df = df.drop_duplicates()
        logger.info("Duplicates removed.")
    return df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleanse and transform the data by handling missing values, duplicates,
    converting 'step' to 'timestamp' and other transformations.

    Args:
        df: Input DataFrame to transform

    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    logger.info("Starting data transformation...")

    # Cleansing
    df = handle_missing_values(df)
    df = handle_duplicates(df)

    # Transformation
    df['timestamp'] = START_DATE + pd.to_timedelta(df['step'], unit='h')
    logger.info("Converted 'step' to 'timestamp'.")

    cols = df.columns.tolist()
    cols.insert(1, cols.pop(cols.index('timestamp')))
    df = df[cols]

    df.to_csv(PROCESSED_DATA_PATH, index=False)
    logger.info(f"Transformed data saved to {PROCESSED_DATA_PATH}")

    return df
