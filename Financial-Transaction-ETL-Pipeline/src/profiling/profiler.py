"""Data profiling module for analyzing and generating reports on dataset characteristics."""

import os
import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import base64
from jinja2 import Environment, FileSystemLoader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_root = str(Path(__file__).parent.parent.parent)
sys.path.append(project_root)

from config.config import DATA_PROFILING_PATH


def load_data(file_path):
    """Load data from CSV file."""
    logger.info(f"Loading data from {file_path}")
    df = pd.read_csv(file_path)
    return df


def compute_descriptive_statistics(df):
    """Compute descriptive statistics for numerical columns."""
    logger.info("Computing descriptive statistics")
    descriptive_stats = df.describe().transpose()
    return descriptive_stats


def analyze_data_types(df):
    """Analyze data types and detect inconsistencies."""
    logger.info("Analyzing data types")
    data_types = df.dtypes.to_frame(name='Data Type')
    return data_types


def detect_missing_values(df):
    """Detect missing values in the dataset."""
    logger.info("Detecting missing values")
    missing_values = df.isnull().sum().to_frame(name='Missing Values')
    missing_values['% Missing'] = (missing_values['Missing Values'] / len(df)) * 100
    return missing_values


def find_unique_values(df):
    """Find number of unique values per column."""
    logger.info("Finding unique values per column")
    unique_values = df.nunique().to_frame(name='Unique Values')
    return unique_values


def detect_duplicates(df):
    """Detect duplicate rows in the dataset."""
    logger.info("Detecting duplicate rows")
    duplicate_rows = df.duplicated()
    num_duplicates = duplicate_rows.sum()
    duplicate_samples = df[duplicate_rows].head()
    return num_duplicates, duplicate_samples


def analyze_value_distributions(df):
    """Analyze value distributions and generate plots or tables."""
    logger.info("Analyzing value distributions")
    numerical_columns = [
        col for col in df.select_dtypes(include=[np.number]).columns.tolist()
        if col not in ['isFraud', 'isFlaggedFraud', 'nameOrig', 'nameDest']
    ]
    categorical_columns = [
        col for col in df.select_dtypes(include=['object', 'category']).columns.tolist()
        if col not in ['isFraud', 'isFlaggedFraud', 'nameOrig', 'nameDest']
    ]

    plots = []

    for col in ['isFraud', 'isFlaggedFraud']:
        if col in df.columns:
            value_counts = df[col].value_counts().to_frame(name='Count')
            table_html = value_counts.to_html(
                classes='table table-striped table-bordered',
                border=0
            )
            plots.append({
                'type': 'table',
                'title': f'Value Counts of {col}',
                'content': table_html
            })

    for col in numerical_columns:
        plt.figure(figsize=(8, 4))
        data = df[col].copy()

        if (data <= 0).any():
            data = data[data > 0]
            if data.empty:
                logger.warning(f"No positive values in {col} to plot on log scale.")
                continue
            transformation = 'log'
        else:
            transformation = 'log'

        if data.skew() > 1:
            data = np.log10(data)
            plt.xlabel(f'Log-scaled {col}')
        else:
            plt.xlabel(col)

        sns.histplot(data, kde=True, bins=30)
        plt.title(f'Distribution of {col}')
        plt.ylabel('Frequency')
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        plt.close()
        buf.seek(0)
        image_base64 = base64.b64encode(buf.read()).decode('utf-8')
        plots.append({
            'type': 'image',
            'title': f'Distribution of {col}',
            'content': image_base64
        })

    for col in categorical_columns:
        plt.figure(figsize=(8, 4))
        value_counts = df[col].value_counts().nlargest(20)  # Show top 20 categories
        sns.barplot(x=value_counts.index, y=value_counts.values)
        plt.title(f'Value Counts of {col}')
        plt.xlabel(col)
        plt.ylabel('Count')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        plt.close()
        buf.seek(0)
        image_base64 = base64.b64encode(buf.read()).decode('utf-8')
        plots.append({
            'type': 'image',
            'title': f'Value Counts of {col}',
            'content': image_base64
        })

    return plots


def compute_correlations(df):
    """Compute correlation matrix for numerical variables."""
    logger.info("Computing correlation matrix")
    numerical_df = df.select_dtypes(include=[np.number])
    correlation_matrix = numerical_df.corr()
    plt.figure(figsize=(12, 10))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', square=True)
    plt.title('Correlation Matrix')
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    plt.close()
    buf.seek(0)
    correlation_plot_base64 = base64.b64encode(buf.read()).decode('utf-8')

    return correlation_matrix, correlation_plot_base64


def detect_outliers(df):
    """Detect outliers using the 90th percentile method and calculate statistics."""
    logger.info("Detecting outliers and calculating statistics")
    outliers = {}
    col = 'amount'

    mean_val = df[col].mean()
    median_val = df[col].median()
    std_val = df[col].std()
    percentile_90 = df[col].quantile(0.90)

    outlier_rows = df[df[col] > percentile_90]

    outliers[col] = {
        'num_outliers': len(outlier_rows),
        'mean': mean_val,
        'median': median_val,
        'std': std_val,
        'percentile_90': percentile_90,
        'outlier_samples': outlier_rows.head().to_dict(orient='records'),
    }
    return outliers


def generate_report(context):
    """Generate the profiling report using an HTML template."""
    logger.info("Generating profiling report")
    env = Environment(loader=FileSystemLoader(searchpath=str(Path(__file__).parent)))
    template = env.get_template('profiling_template.html')

    report_html = template.render(context)

    output_path = Path(DATA_PROFILING_PATH)
    output_path.mkdir(parents=True, exist_ok=True)
    report_file = output_path / 'profiling_report.html'
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report_html)
    logger.info(f"Profiling report saved to {report_file}")


def profile_data(df: pd.DataFrame) -> None:
    """Main function to perform data profiling."""

    descriptive_stats = compute_descriptive_statistics(df)
    data_types = analyze_data_types(df)
    missing_values = detect_missing_values(df)
    unique_values = find_unique_values(df)
    num_duplicates, duplicate_samples = detect_duplicates(df)
    plots = analyze_value_distributions(df)
    correlation_matrix, correlation_plot_base64 = compute_correlations(df)
    outliers = detect_outliers(df)

    context = {
        'descriptive_stats': descriptive_stats.to_html(
            classes='table table-striped table-bordered',
            border=0
        ),
        'data_types': data_types.to_html(
            classes='table table-striped table-bordered',
            border=0
        ),
        'missing_values': missing_values.to_html(
            classes='table table-striped table-bordered',
            border=0
        ),
        'unique_values': unique_values.to_html(
            classes='table table-striped table-bordered',
            border=0
        ),
        'num_duplicates': num_duplicates,
        'duplicate_samples': duplicate_samples.to_html(
            classes='table table-striped table-bordered',
            border=0
        ),
        'plots': plots,
        'correlation_matrix': correlation_matrix.to_html(
            classes='table table-striped table-bordered',
            border=0
        ),
        'correlation_plot': correlation_plot_base64,
        'outliers': outliers,
    }

    generate_report(context)
