"""Module for downloading and extracting the Kaggle dataset."""
import pandas as pd
import logging
import os
import sys
import zipfile
from pathlib import Path

from kaggle.api.kaggle_api_extended import KaggleApi

# Add the project root directory to Python path to import config
project_root = str(Path(__file__).parent.parent.parent)
sys.path.append(project_root)

from config.config import KAGGLE_DATASET, KAGGLE_FILENAME, RAW_DATA_PATH


def download_dataset_from_kaggle():
    """Download the Kaggle dataset if not already present in raw data directory."""
    if os.path.exists(RAW_DATA_PATH):
        logging.info(f"Raw data already exists at {RAW_DATA_PATH}. Skipping download.")
        return

    logging.info(f"Raw data not found at {RAW_DATA_PATH}. Starting download.")

    api = KaggleApi()
    api.authenticate()

    raw_data_dir = os.path.dirname(RAW_DATA_PATH)
    os.makedirs(raw_data_dir, exist_ok=True)

    try:
        api.dataset_download_file(
            dataset=KAGGLE_DATASET,
            file_name=KAGGLE_FILENAME,
            path=raw_data_dir,
            force=False,
            quiet=False
        )

        zip_file_path = os.path.join(raw_data_dir, f"{KAGGLE_FILENAME}.zip")
        if os.path.exists(zip_file_path):
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(raw_data_dir)
            os.remove(zip_file_path)
            logging.info(f"Successfully downloaded and extracted dataset to {raw_data_dir}")
        else:
            raise FileNotFoundError(f"Zip file {zip_file_path} not found after download.")

    except Exception as e:
        logging.error(f"Failed to download dataset: {e}")
        raise


def load_raw_dataset() -> pd.DataFrame:
    """Load the raw dataset from disk into a pandas DataFrame.
    
    Returns:
        pd.DataFrame: The loaded raw dataset
    """
    if not os.path.exists(RAW_DATA_PATH):
        download_dataset_from_kaggle()
        
    try:
        df = pd.read_csv(RAW_DATA_PATH)
        logging.info(f"Successfully loaded dataset from {RAW_DATA_PATH}")
        return df
    except Exception as e:
        logging.error(f"Failed to load dataset: {e}")
        raise

