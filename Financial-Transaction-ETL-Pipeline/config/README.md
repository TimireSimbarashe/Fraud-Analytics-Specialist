# Financial Transaction ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for processing and analyzing financial transaction data. This pipeline downloads transaction data from Kaggle, performs data validation, profiling, transformation, quality checks, and aggregation.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Pipeline Components](#pipeline-components)
- [Output](#output)

## Overview

This pipeline processes financial transaction data through several stages:
1. Downloads raw transaction data from Kaggle
2. Profiles the data to understand its characteristics
3. Validates the data structure and content
4. Transforms the data into a standardized format
5. Performs quality checks
6. Aggregates the data for analysis

## Features

- Automated data download from Kaggle
- Comprehensive data profiling with visualizations
- Data validation and quality checks
- Data transformation and standardization
- Aggregation for analysis
- Detailed logging and error handling
- HTML report generation

## Prerequisites

- Python 3.7+
- Kaggle account and API credentials
- Required Python packages (install via requirements.txt)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/Financial-Transaction-ETL-Pipeline.git
   cd Financial-Transaction-ETL-Pipeline
   ```

2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up Kaggle authentication:
   - Create a Kaggle account at [kaggle.com](https://www.kaggle.com)
   - Generate API credentials from your account settings
   - Place the `kaggle.json` file in:
     - Linux/MacOS: `~/.kaggle/kaggle.json`
     - Windows: `C:\Users\<Username>\.kaggle\kaggle.json`
   - Set appropriate permissions (Linux/MacOS):
     ```bash
     chmod 600 ~/.kaggle/kaggle.json
     ```

## Usage

1. Configure the pipeline settings in `src/config/settings.py`:
   - Set the Kaggle dataset details
   - Configure input/output paths
   - Adjust validation rules if needed

2. Run the complete pipeline:
   ```bash
   python -m src.run_pipeline
   ```

3. View the outputs:
   - Raw data: `data/raw/`
   - Profiling report: `data/profiling/profiling_report.html`
   - Transformed data: `data/processed/`
   - Aggregated results: `data/aggregated/`

## Pipeline Components

The pipeline consists of several key components:

1. Data Extraction (`src/etl/extract.py`)
   - Downloads dataset from Kaggle
   - Loads raw data into pandas DataFrame

2. Data Profiling (`src/profiling/profiler.py`)
   - Generates statistical summaries
   - Creates visualizations
   - Produces HTML profiling report

3. Data Validation (`src/validation/validators.py`)
   - Checks data structure
   - Validates data types
   - Ensures data quality

4. Data Transformation (`src/etl/transform.py`)
   - Standardizes formats
   - Handles missing values
   - Performs feature engineering

5. Quality Checks (`src/quality_checks/quality_checks.py`)
   - Verifies transformation results
   - Ensures data integrity
   - Validates business rules

6. Data Aggregation (`src/aggregation/aggregator.py`)
   - Summarizes transactions
   - Generates insights
   - Prepares analysis-ready datasets
