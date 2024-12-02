"""Data validation module for checking schema and values."""

import logging
from pathlib import Path
import sys

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_root = str(Path(__file__).parent.parent.parent)
sys.path.append(project_root)

from config.config import SCHEMA_CONFIG


def validate_schema(df: pd.DataFrame) -> bool:
    """
    Validate the DataFrame against the expected schema.

    Args:
        df: DataFrame to validate

    Returns:
        bool: True if schema is valid, False otherwise
    """
    logger.info("Validating data schema...")
    missing_columns = set(SCHEMA_CONFIG.keys()) - set(df.columns)
    if missing_columns:
        logger.error(f"Missing columns: {missing_columns}")
        return False

    for column, expected_type in SCHEMA_CONFIG.items():
        if df[column].dtype != expected_type:
            logger.error(
                f"Column {column} has incorrect type {df[column].dtype}, "
                f"expected {expected_type}."
            )
            return False

    logger.info("Data schema validation passed.")
    return True


def validate_values(df: pd.DataFrame) -> bool:
    """
    Validate the values within the DataFrame.

    Args:
        df: DataFrame to validate

    Returns:
        bool: True if values are valid, False otherwise
    """
    logger.info("Validating data values...")

    if (df['amount'] < 0).any():
        logger.error("Negative values found in 'amount' column.")
        return False

    if not df['isFraud'].isin([0, 1]).all():
        logger.error("Non-binary values found in 'isFraud' column.")
        return False

    if not df['isFlaggedFraud'].isin([0, 1]).all():
        logger.error("Non-binary values found in 'isFlaggedFraud' column.")
        return False

    logger.info("Data values validation passed.")
    return True


def validate_data(df: pd.DataFrame) -> bool:
    """
    Main function to perform data validation.

    Returns:
        bool: True if all validations pass, False otherwise
    """
    schema_valid = validate_schema(df)
    values_valid = validate_values(df)
    return schema_valid and values_valid
