
import logging
from src import (
    download_dataset_from_kaggle,
    load_raw_dataset,
    profile_data,
    validate_data,
    transform_data,
    run_quality_checks,
    aggregate_data
)


def run_etl_pipeline():
    """Run the complete ETL pipeline.
    
    Returns:
        bool: True if pipeline completes successfully, False otherwise.
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        download_dataset_from_kaggle()
        logger.info("Data extraction completed")
        
        raw_data = load_raw_dataset()
        logger.info("Data loading completed")

        profile_data(raw_data)
        logger.info("Data profiling completed")

        validate_data(raw_data)
        logger.info("Data validation completed")

        transformed_data = transform_data(raw_data)
        logger.info("Data transformation completed")

        run_quality_checks(transformed_data)
        logger.info("Quality checks completed")

        aggregate_data(transformed_data)
        logger.info("Data aggregation completed")

        logger.info("ETL pipeline completed successfully")
        return True

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        return False