"""Module for aggregating transaction data into useful features for fraud detection."""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_root = str(Path(__file__).parent.parent.parent)
sys.path.append(project_root)

from config.config import (
    AGGREGATED_DATA_DAILY_PATH,
    AGGREGATED_DATA_TIME_OF_DAY_PATH,
    AGGREGATED_DATA_WEEKLY_PATH
)


def ensure_path_exists(path):
    """Create directory if it doesn't exist."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def get_time_of_day(hour):
    """Categorize hour into day or night.
    
    Args:
        hour (int): Hour of day (0-23)
        
    Returns:
        str: 'day' if between 6am-6pm, 'night' otherwise
    """
    if 6 <= hour < 18:
        return 'day'
    return 'night'


def aggregate_data(df: pd.DataFrame) -> None:
    """Aggregate transaction data to create features useful for fraud detection.
    
    Aggregates data by day, week and time of day for origin accounts.
    Does not use rolling windows.
    """
    ensure_path_exists(AGGREGATED_DATA_DAILY_PATH)
    ensure_path_exists(AGGREGATED_DATA_WEEKLY_PATH)
    ensure_path_exists(AGGREGATED_DATA_TIME_OF_DAY_PATH)

    logger.info("Starting data aggregation...")

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    df['date'] = df['timestamp'].dt.date
    df['hour'] = df['timestamp'].dt.hour
    df['week'] = df['timestamp'].dt.isocalendar().week
    df['year'] = df['timestamp'].dt.year

    df['time_of_day'] = df['hour'].apply(get_time_of_day)

    logger.info("Aggregating data for origin accounts...")

    origin_daily_agg = df.groupby(['nameOrig', 'date']).agg(
        total_transactions=('amount', 'count'),
        total_amount=('amount', 'sum'),
        avg_amount=('amount', 'mean'),
        max_amount=('amount', 'max'),
        min_amount=('amount', 'min'),
        std_amount=('amount', 'std'),
        total_fraud_transactions=('isFraud', 'sum'),
        total_fraud_amount=(
            'amount',
            lambda x: x[df.loc[x.index, 'isFraud'] == 1].sum()
        )
    ).reset_index()

    daily_path = AGGREGATED_DATA_DAILY_PATH.replace('.csv', '_origin.csv')
    origin_daily_agg.to_csv(daily_path, index=False)
    logger.info(f"Origin daily aggregation saved to {daily_path}")

    origin_weekly_agg = df.groupby(['nameOrig', 'year', 'week']).agg(
        total_transactions=('amount', 'count'),
        total_amount=('amount', 'sum'),
        avg_amount=('amount', 'mean'),
        max_amount=('amount', 'max'),
        min_amount=('amount', 'min'),
        std_amount=('amount', 'std'),
        total_fraud_transactions=('isFraud', 'sum'),
        total_fraud_amount=(
            'amount',
            lambda x: x[df.loc[x.index, 'isFraud'] == 1].sum()
        )
    ).reset_index()

    weekly_path = AGGREGATED_DATA_WEEKLY_PATH.replace('.csv', '_origin.csv')
    origin_weekly_agg.to_csv(weekly_path, index=False)
    logger.info(f"Origin weekly aggregation saved to {weekly_path}")

    origin_time_of_day_agg = df.groupby(['nameOrig', 'date', 'time_of_day']).agg(
        total_transactions=('amount', 'count'),
        total_amount=('amount', 'sum'),
        avg_amount=('amount', 'mean'),
        total_fraud_transactions=('isFraud', 'sum')
    ).reset_index()

    time_path = AGGREGATED_DATA_TIME_OF_DAY_PATH.replace('.csv', '_origin.csv')
    origin_time_of_day_agg.to_csv(time_path, index=False)
    logger.info(f"Origin time of day aggregation saved to {time_path}")

    logger.info("Data aggregation completed successfully.")
