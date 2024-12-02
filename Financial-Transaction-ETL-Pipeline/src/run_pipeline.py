"""Main entry point for running the ETL pipeline."""

import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

if __name__ == "__main__":
    from src.pipeline import run_etl_pipeline
    success = run_etl_pipeline()
    if success:
        logger.info("Pipeline completed successfully")
        sys.exit(0)
    else:
        logger.error("Pipeline failed")
        sys.exit(1)
