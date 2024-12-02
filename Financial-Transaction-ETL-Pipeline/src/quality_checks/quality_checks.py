"""Quality checks module for validating data consistency and correctness."""

import logging
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from jinja2 import Environment, FileSystemLoader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_root = str(Path(__file__).parent.parent.parent)
sys.path.append(project_root)

from config.config import (
    SCHEMA_CONFIG,
    DATA_QUALITY_REPORT_PATH,
)


def check_balance_consistency(df: pd.DataFrame) -> tuple[bool, dict]:
    """Check if transaction balances are consistent based on transaction type.

    Args:
        df: DataFrame containing transaction data

    Returns:
        tuple containing:
            - bool: True if all balances are consistent, False otherwise
            - dict: Dictionary of inconsistent transactions by type
    """
    logger.info("Checking balance consistency based on transaction types...")
    inconsistencies = {}
    passed = True

    cash_in = df[df['type'] == 'CASH_IN']
    inconsistent_cash_in = cash_in[
        (cash_in['newbalanceOrig'] != cash_in['oldbalanceOrg'] + cash_in['amount'])
    ]
    if not inconsistent_cash_in.empty:
        inconsistencies['CASH_IN'] = inconsistent_cash_in
        logger.warning(
            f"Found {len(inconsistent_cash_in)} inconsistencies in CASH_IN transactions."
        )
        passed = False

    cash_out = df[df['type'] == 'CASH_OUT']
    inconsistent_cash_out = cash_out[
        (cash_out['newbalanceOrig'] != cash_out['oldbalanceOrg'] - cash_out['amount'])
    ]
    if not inconsistent_cash_out.empty:
        inconsistencies['CASH_OUT'] = inconsistent_cash_out
        logger.warning(
            f"Found {len(inconsistent_cash_out)} inconsistencies in CASH_OUT transactions."
        )
        passed = False

    debit = df[df['type'] == 'DEBIT']
    inconsistent_debit = debit[
        (debit['newbalanceOrig'] != debit['oldbalanceOrg'] - debit['amount'])
    ]
    if not inconsistent_debit.empty:
        inconsistencies['DEBIT'] = inconsistent_debit
        logger.warning(
            f"Found {len(inconsistent_debit)} inconsistencies in DEBIT transactions."
        )
        passed = False

    payment = df[df['type'] == 'PAYMENT']
    inconsistent_payment = payment[
        (payment['newbalanceOrig'] != payment['oldbalanceOrg'] - payment['amount']) |
        (payment['newbalanceDest'] != payment['oldbalanceDest'] + payment['amount'])
    ]
    if not inconsistent_payment.empty:
        inconsistencies['PAYMENT'] = inconsistent_payment
        logger.warning(
            f"Found {len(inconsistent_payment)} inconsistencies in PAYMENT transactions."
        )
        passed = False

    transfer = df[df['type'] == 'TRANSFER']
    inconsistent_transfer = transfer[
        (transfer['newbalanceOrig'] != transfer['oldbalanceOrg'] - transfer['amount']) |
        (transfer['newbalanceDest'] != transfer['oldbalanceDest'] + transfer['amount'])
    ]
    if not inconsistent_transfer.empty:
        inconsistencies['TRANSFER'] = inconsistent_transfer
        logger.warning(
            f"Found {len(inconsistent_transfer)} inconsistencies in TRANSFER transactions."
        )
        passed = False

    if passed:
        logger.info("All transactions have consistent balances.")
    else:
        logger.info("Balance inconsistencies found in transactions.")

    return passed, inconsistencies


def check_value_ranges(df: pd.DataFrame) -> tuple[bool, dict]:
    """Check if values in columns are within expected ranges.

    Args:
        df: DataFrame containing transaction data

    Returns:
        tuple containing:
            - bool: True if all values are within expected ranges, False otherwise
            - dict: Dictionary of issues found by column
    """
    logger.info("Checking expected values in columns...")
    issues = {}
    passed = True

    for col in ['isFraud', 'isFlaggedFraud']:
        if col in df.columns:
            invalid_values = df[~df[col].isin([0, 1])]
            if not invalid_values.empty:
                issues[col] = {
                    'description': f"Column '{col}' contains values other than 0 or 1.",
                    'invalid_rows': invalid_values
                }
                logger.warning(f"Found invalid values in column '{col}'.")
                passed = False

    expected_types = ['PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT', 'CASH_IN']
    if 'type' in df.columns:
        invalid_types = df[~df['type'].isin(expected_types)]
        if not invalid_types.empty:
            issues['type'] = {
                'description': "Column 'type' contains unexpected values.",
                'invalid_rows': invalid_types
            }
            logger.warning("Found unexpected values in column 'type'.")
            passed = False

    return passed, issues


def check_data_types(df: pd.DataFrame, schema: dict) -> tuple[bool, dict]:
    """Check if column data types match expected schema.

    Args:
        df: DataFrame containing transaction data
        schema: Dictionary mapping column names to expected data types

    Returns:
        tuple containing:
            - bool: True if all types match schema, False otherwise
            - dict: Dictionary of type mismatches by column
    """
    logger.info("Checking data types of columns...")
    incorrect_types = {}
    passed = True

    for column, expected_type in schema.items():
        if column in df.columns:
            actual_dtype = df[column].dtype
            if not np.issubdtype(actual_dtype, expected_type):
                incorrect_types[column] = {
                    'expected': expected_type.__name__,
                    'actual': actual_dtype.name
                }
                logger.warning(
                    f"Column '{column}' has incorrect type '{actual_dtype}', "
                    f"expected '{expected_type.__name__}'."
                )
                passed = False
        else:
            logger.warning(f"Column '{column}' is missing from the DataFrame.")
            incorrect_types[column] = {
                'expected': expected_type.__name__,
                'actual': 'Column not found'
            }
            passed = False

    return passed, incorrect_types


def generate_data_quality_report(context: dict) -> None:
    """Generate HTML report of data quality check results.

    Args:
        context: Dictionary containing check results and issues found
    """
    logger.info("Generating data quality report...")

    env = Environment(loader=FileSystemLoader(searchpath=str(Path(__file__).parent)))
    template = env.get_template('data_quality_template.html')

    report_html = template.render(context)

    output_path = Path(DATA_QUALITY_REPORT_PATH)
    report_file = output_path / 'data_quality_report.html'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report_html)
    logger.info(f"Data quality report saved to {report_file}")


def run_quality_checks(df: pd.DataFrame) -> None:
    """Run all data quality checks and generate report."""
    data_quality_issues = {}
    check_results = {}

    data_types_passed, incorrect_types = check_data_types(df, SCHEMA_CONFIG)
    if not data_types_passed:
        data_quality_issues['incorrect_types'] = incorrect_types
        check_results['Data Type Checks'] = 'Failed'
    else:
        check_results['Data Type Checks'] = 'Passed'

    value_ranges_passed, value_issues = check_value_ranges(df)
    if not value_ranges_passed:
        data_quality_issues['value_issues'] = value_issues
        check_results['Value Range Checks'] = 'Failed'
    else:
        check_results['Value Range Checks'] = 'Passed'

    balance_passed, balance_inconsistencies = check_balance_consistency(df)
    if not balance_passed:
        data_quality_issues['balance_inconsistencies'] = balance_inconsistencies
        check_results['Balance Consistency Checks'] = 'Failed'
    else:
        check_results['Balance Consistency Checks'] = 'Passed'

    if data_quality_issues:
        logger.info("Data quality issues found and they have been logged in the data quality report.")
    else:
        logger.info("No data quality issues found.")

    context = {
        'check_results': check_results,
        'data_quality_issues': data_quality_issues
    }
    generate_data_quality_report(context)
