import os
import numpy as np
from datetime import datetime

SCHEMA_CONFIG = {
    'step': np.int64,
    'type': object,
    'amount': np.float64,
    'nameOrig': object,
    'oldbalanceOrg': np.float64,
    'newbalanceOrig': np.float64,
    'nameDest': object,
    'oldbalanceDest': np.float64,
    'newbalanceDest': np.float64,
    'isFraud': np.int64,
    'isFlaggedFraud': np.int64
}

# Data paths
RAW_DATA_PATH = os.path.join(
    'data', 'raw', 'PS_20174392719_1491204439457_log.csv')
PROCESSED_DATA_PATH = os.path.join(
    'data', 'processed', 'transactions_processed.csv')

AGGREGATED_DATA_HOURLY_PATH = os.path.join(
    'data', 'processed', 'aggregated_data_hourly.csv')
AGGREGATED_DATA_DAILY_PATH = os.path.join(
    'data', 'processed', 'aggregated_data_daily.csv')
AGGREGATED_DATA_WEEKLY_PATH = os.path.join(
    'data', 'processed', 'aggregated_data_weekly.csv')
AGGREGATED_DATA_TIME_OF_DAY_PATH = os.path.join(
    'data', 'processed', 'aggregated_data_time_of_day.csv')
AGGREGATED_DATA_BY_TYPE_PATH = os.path.join(
    'data', 'processed', 'aggregated_data_by_type.csv')

DATA_PROFILING_PATH = os.path.join('docs', 'data_profiling')
DATA_QUALITY_REPORT_PATH = os.path.join('docs', 'reports')

# Kaggle dataset details
KAGGLE_DATASET = 'ealaxi/paysim1'
KAGGLE_FILENAME = 'PS_20174392719_1491204439457_log.csv'

# Logging configuration
LOGGING_LEVEL = 'INFO'

START_DATE = datetime(2022, 1, 1)




