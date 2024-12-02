"""Module for downloading and extracting Kaggle datasets."""

import logging
import os
import zipfile

from kaggle.api.kaggle_api_extended import KaggleApi

def download_dataset_from_kaggle(raw_data_path: str, kaggle_dataset: str, kaggle_filename: str):
    """Download the Kaggle dataset if not already present in raw data directory."""
    if os.path.exists(raw_data_path):
        logging.info("Raw data already exists at %s. Skipping download.", raw_data_path)
        return

    logging.info("Raw data not found at %s. Starting download.", raw_data_path)

    api = KaggleApi()
    api.authenticate()

    raw_data_dir = os.path.dirname(raw_data_path)
    os.makedirs(raw_data_dir, exist_ok=True)

    try:
        api.dataset_download_file(
            dataset=kaggle_dataset,
            file_name=kaggle_filename,
            path=raw_data_dir,
            force=False,
            quiet=False,
        )

        zip_file_path = os.path.join(raw_data_dir, f"{kaggle_filename}.zip")
        if os.path.exists(zip_file_path):
            with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                zip_ref.extractall(raw_data_dir)
            os.remove(zip_file_path)
            logging.info(
                "Successfully downloaded and extracted dataset to %s", raw_data_dir
            )
        else:
            raise FileNotFoundError(
                f"Zip file {zip_file_path} not found after download."
            )

    except Exception as e:
        logging.error("Failed to download dataset: %s", e)
        raise