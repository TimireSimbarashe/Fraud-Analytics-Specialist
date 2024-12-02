# Large-Scale Data Processing Pipeline

A comprehensive data pipeline for processing and analyzing credit card fraud detection data using Apache Spark.

## Project Overview

This project implements a scalable data processing pipeline using Apache Spark that:
- Downloads credit card transaction data from Kaggle
- Leverages Spark's distributed computing for efficient data cleaning and preprocessing
- Applies feature engineering and transformations across partitioned data
- Generates detailed data quality reports at each stage by processing data in parallel

## Architecture

The pipeline consists of several key components:

1. **Data Extraction** (`data_extraction.py`)
   - Downloads dataset from Kaggle using their API
   - Handles authentication and data retrieval

2. **Data Preparation** (`data_preparation.py`) 
   - Ingests raw CSV data using PySpark
   - Handles data cleaning including:
     - Deduplication
     - Missing value imputation
     - Data type conversions

3. **Data Transformation** (`data_transformation.py`)
   - Applies feature engineering including:
     - Temporal features (hour of day, day of week)
     - Transaction timing features
     - Rolling window statistics
     - Amount-based features
     - Statistical measures (z-scores, skewness, kurtosis)

4. **Data Quality Checks** (`data_quality_checks.py`)
   - Generates comprehensive HTML reports containing:
     - Basic data statistics
     - Missing value analysis
     - Schema validation
     - Duplicate checks
     - Summary statistics

## Setup Instructions

### Prerequisites

1. Python 3.7+
2. Apache Spark 3.0+
3. Kaggle account and API credentials

### Installation

1. Clone the repository:
2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure Kaggle credentials:
   - Create a Kaggle account if you don't have one
   - Download your Kaggle API credentials from your account settings
   - Place the `kaggle.json` file in `~/.kaggle/`
   - Set appropriate permissions:
     ```bash
     chmod 600 ~/.kaggle/kaggle.json
     ```
4. Install and configure Hadoop:
   - Download Hadoop from the Apache website
   - Extract to desired location
   - Set HADOOP_HOME environment variable:
     ```bash
     export HADOOP_HOME=/path/to/hadoop
     export PATH=$PATH:$HADOOP_HOME/bin
     ```
   - Configure core-site.xml and hdfs-site.xml as needed
5. Set up Spark environment variables:
   ```bash
   export SPARK_HOME=/path/to/spark
   export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
   export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
   ```

6. Run the pipeline:
   ```bash
   cd Large-Scale-Data-Processing
   python src/run_pipeline.py
   ```
