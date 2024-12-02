# Time-Series Analysis for Fraud Detection

## Table of Contents

1. [Introduction](#introduction)
2. [Dataset Overview](#dataset-overview)
3. [Project Structure](#project-structure)
4. [Data Preparation](#data-preparation)
5. [Exploratory Data Analysis (EDA)](#exploratory-data-analysis-eda)
6. [Feature Engineering](#feature-engineering)
7. [Anomaly Detection](#anomaly-detection)
8. [Key Findings](#key-findings)
9. [Conclusion](#conclusion)
10. [Usage](#usage)
11. [Dependencies](#dependencies)
12. [References](#references)

---

## Introduction

Welcome to the **Time-Series Analysis for Fraud Detection** project. This analysis aims to uncover temporal patterns in fraudulent transactions, leveraging time-based features and statistical methods to identify potential fraud indicators. While this study provides valuable insights, it serves as a foundational exploration and is not comprehensive enough for real-world applications, such as those conducted by financial institutions like the World Bank. Implementing an effective fraud detection system requires in-depth collaboration with domain experts, understanding of business operations, and a robust feature engineering and model deployment pipeline.

---

## Dataset Overview

The dataset used in this analysis is sourced from the [IEEE-CIS Fraud Detection](https://www.kaggle.com/c/ieee-fraud-detection/data) competition on Kaggle. The competition focuses on predicting the probability that an online transaction is fraudulent, as indicated by the binary target variable `isFraud`.

### Dataset Description

- **Transaction Data (`train_transaction.csv`, `test_transaction.csv`)**:
  - **Categorical Features**:
    - `ProductCD`
    - `card1` to `card6`
    - `addr1`, `addr2`
    - `P_emaildomain`
    - `R_emaildomain`
    - `M1` to `M9`
  - **Numerical Features**:
    - `TransactionDT`: A timedelta from a given reference datetime.
    - `TransactionAmt`

- **Identity Data (`train_identity.csv`, `test_identity.csv`)**:
  - **Categorical Features**:
    - `DeviceType`
    - `DeviceInfo`
    - `id_12` to `id_38`

Not all transactions have corresponding identity information. The training set contains labeled data, while the test set requires predictions for the `isFraud` value.

### Selected Features for Analysis

For this analysis, a subset of relevant features was selected to focus on temporal patterns:

- `TransactionID`
- `isFraud`
- `TransactionDT`
- `TransactionAmt`
- `card1`, `card2`, `card3`, `card4`, `card5`, `card6`
- `addr1`, `addr2`
- `P_emaildomain`

---

## Project Structure

The project is organized as follows:

```
Large-Scale-Data-Processing/
├── code/
│   └── time_series_analysis.ipynb
├── data/
│   ├── raw/
│   │   ├── train_transaction.csv
│   │   ├── train_identity.csv
│   │   ├── test_transaction.csv
│   │   └── test_identity.csv
├── reports/
│   ├── analysis_report.html
│   └── analysis_report.pdf
└── README.md
```

- **code/**: Contains the Jupyter notebook `time_series_analysis.ipynb` with the complete analysis.
- **data/**: Houses the raw data extracted from Kaggle, organized into `train` and `test` subdirectories.
- **reports/**: Includes exported versions of the notebook in HTML and PDF formats.
- **README.md**: Provides an overview and guidance for the project.

---

## Data Preparation

Data preparation is a crucial step to ensure the quality and usability of the dataset for analysis. The following steps were undertaken:

1. **Data Ingestion**:
    - Read transaction and identity CSV files using `pandas`.
    - Merge the transaction and identity data on `TransactionID` using a left join.

2. **Handling Missing Values**:
    - **Numeric Columns**:
        - Identified numerical columns and filled missing values with the median.
    - **Categorical Columns**:
        - Identified categorical columns and filled missing values with the mode (most frequent value).
    - **Dropping Columns**:
        - Removed columns with more than 90% missing values as they provide limited information.

3. **Selecting Relevant Features**:
    - Focused on a subset of features pertinent to temporal analysis and basic transaction details.

4. **Extracting Temporal Features**:
    - Converted `TransactionDT` to datetime features relative to a defined `START_DATE`.
    - Extracted `TransactionDate`, `TransactionDay`, `TransactionHour`, `TransactionDayOfWeek`, `TransactionWeekOfYear`, and `TransactionMonth`.

5. **Creating Unique Card Identifiers**:
    - Combined card-related features to create a unique identifier `uid` for each card.

6. **Additional Time-Based Features**:
    - **Card Tenure**:
        - Calculated `DaysSinceFirstTransaction` and `DaysSinceLastTransaction` for each card.
    - **Time Since Last Transaction**:
        - Computed the time difference between consecutive transactions for the same card.

---

## Exploratory Data Analysis (EDA)

EDA was performed to uncover initial patterns and insights into the dataset, focusing on temporal aspects of fraudulent transactions.

### Temporal Patterns in Fraudulent Transactions

#### Fraud Rate by Hour of Day

- **Objective**: Determine if fraud rates vary across different hours of the day.
- **Findings**:
    - Higher fraud rates observed during early morning hours (6 AM to 9 AM).
    - Possible rationale: Fraudsters may exploit lower staffing levels or reduced monitoring during these hours.

![Fraud Rate by Hour of Day](./reports/fraud_rate_by_hour.png)

*No description provided for the image.*

#### Fraud Rate by Day of Week

- **Objective**: Assess if certain days of the week have higher fraud rates.
- **Findings**:
    - Varying fraud rates across different days, with some days exhibiting higher tendencies.
    - Requires further investigation to understand underlying causes.

![Fraud Rate by Day of Week](./reports/fraud_rate_by_day_of_week.png)

*No description provided for the image.*

#### Card-Level Temporal Analysis

- **Objective**: Explore transaction intervals for fraudulent vs. non-fraudulent transactions.
- **Hypothesis**: Fraudulent transactions occur in rapid succession, indicating automated or suspicious activity.
- **Findings**:
    - Fraudulent transactions have significantly shorter time intervals between them compared to non-fraudulent ones.
    - Statistical significance confirmed with a t-test (T-statistic: -22.450, P-value: 2.019e-110).

![Time Between Transactions](./reports/time_between_transactions.png)

*No description provided for the image.*

---

## Feature Engineering

Advanced feature engineering was implemented to capture temporal dynamics and transaction behaviors conducive to fraud detection.

### Rolling Statistics (Per Card)

- **Features**:
    - **3-Transaction Window**:
        - `TransactionAmt_RollingMean_3`: Mean transaction amount over the past 3 transactions.
        - `TransactionAmt_RollingStd_3`: Standard deviation of transaction amounts over the past 3 transactions.
    - **7-Transaction Window**:
        - `TransactionAmt_RollingMean_7`: Mean transaction amount over the past 7 transactions.
        - `TransactionAmt_RollingStd_7`: Standard deviation of transaction amounts over the past 7 transactions.
- **Purpose**: Detect sudden changes in spending patterns.

### Time-Based Aggregations

- **Features**:
    - `DailyTransactionAmt`: Total transaction amount per day.
    - `DailyTransactionCount`: Number of transactions per day.
    - `WeeklyTransactionAmt`: Total transaction amount per week.
    - `WeeklyTransactionCount`: Number of transactions per week.
- **Purpose**: Capture regular spending cycles and volume patterns.

### Rolling Skewness and Kurtosis

- **Features**:
    - `TransactionAmt_RollingSkew_3`: Skewness of transaction amounts over the past 3 transactions.
    - `TransactionAmt_RollingKurtosis_3`: Kurtosis of transaction amounts over the past 3 transactions.
- **Purpose**: Identify asymmetry and outliers in transaction amounts, which may indicate fraud.

![Rolling Mean Distribution](./reports/rolling_mean_distribution.png)
*No description provided for the image.*

![Rolling Std Distribution](./reports/rolling_std_distribution.png)
*No description provided for the image.*

### Ratio-Based Features

- **Feature**:
    - `TransactionAmt_to_RollingMean_3`: Ratio of current transaction amount to the rolling mean over 3 transactions.
- **Purpose**: Identify transactions significantly higher than historical averages, signaling potential anomalies.

![Transaction Amount Ratio Distribution](./reports/transaction_amt_ratio_distribution.png)
*No description provided for the image.*

---

## Anomaly Detection

### Z-Score for Transaction Amounts

- **Objective**: Identify transactions that deviate significantly from the mean transaction amount for each card.
- **Method**:
    - Calculated the z-score for `TransactionAmt` relative to each card's mean and standard deviation.
    - Classified transactions with an absolute z-score > 3 as anomalies.
- **Findings**:
    - **Number of Anomalies**: 8,036
    - **Fraud Rate Among Anomalies**: 5.01%
    - **Fraud Rate Among Non-Anomalies**: 3.55%
    - **Statistical Significance**: T-statistic = 5.997, P-value = 2.099e-09
    - **Conclusion**: Anomalous transactions have a significantly higher fraud rate, validating the effectiveness of z-score-based anomaly detection.

---

## Key Findings

The analysis yielded several insightful findings regarding temporal patterns in fraudulent transactions:

1. **Higher Fraud Rates During Early Morning Hours**:
    - Fraudulent transactions peak between 6 AM to 9 AM.
    - Potential exploitation of low monitoring periods by fraudsters.

2. **Rapid Succession of Fraudulent Transactions**:
    - Fraudulent transactions occur with significantly shorter intervals.
    - Indicates possible automated or bot-driven activities.

3. **Sudden Increases in Transaction Amounts**:
    - Elevated rolling mean and standard deviation in transaction amounts signal abnormal spending.
    - Transactions significantly higher than the rolling mean are more prone to fraud.

4. **Distinct Skewness and Kurtosis in Fraudulent Transactions**:
    - Fraudulent transactions exhibit different distribution shapes compared to legitimate ones.
    - Asymmetry and presence of outliers as indicated by skewness and kurtosis measures.

5. **Influence of Card Tenure**:
    - The tenure of a card affects its transaction patterns.
    - Newer cards may have higher fraud risks, while unusual activity on long-standing cards is also suspicious.

6. **Effectiveness of Z-Score-Based Anomaly Detection**:
    - Anomalous transactions (based on z-score) have a higher fraud rate.
    - Serves as a robust feature for enhancing fraud detection models.

---

## Conclusion

This time-series analysis highlights the significance of temporal features in detecting fraudulent transactions. By engineering rolling statistics, analyzing transaction intervals, and assessing deviations from typical spending patterns, the analysis provides valuable insights into fraud behaviors. These findings underscore the potential of advanced feature engineering and statistical measures in enhancing the accuracy and reliability of fraud detection systems.

**Limitations**:

- Focused solely on temporal aspects, excluding spatial and other transactional features.
- Lacked a comprehensive model training and evaluation pipeline.
- Did not incorporate domain-specific knowledge or collaborate with fraud analysts.

**Future Work**:

- Integrate additional features for a holistic fraud detection model.
- Implement machine learning models leveraging engineered features.
- Collaborate with domain experts to validate findings and refine detection strategies.

---

## Usage

To replicate the analysis, follow the steps below:

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/Time-Series-Fraud-Detection.git
cd Time-Series-Fraud-Detection
```

### 2. Install Dependencies

Ensure you have Python 3.7 or higher installed. Install the required packages using `pip`:

```bash
pip install -r requirements.txt
```

### 3. Data Preparation

Download the dataset from [Kaggle](https://www.kaggle.com/c/ieee-fraud-detection/data) and place the CSV files in the `data/raw/` directory:

```
data/
├── raw/
│   ├── train_transaction.csv
│   ├── train_identity.csv
│   ├── test_transaction.csv
│   └── test_identity.csv
```

### 4. Run the Jupyter Notebook

Launch Jupyter Notebook and open `time_series_analysis.ipynb`:

```bash
jupyter notebook
```

Navigate to the notebook and execute the cells sequentially.

### 5. Review Reports

After running the notebook, exported reports in HTML and PDF formats can be found in the `reports/` directory:

```
reports/
├── analysis_report.html
└── analysis_report.pdf
```

---
### Installing Dependencies

All dependencies are listed in the `requirements.txt` file. Install them using:

```bash
pip install -r requirements.txt
```

---

## References

1. **Kaggle Competition**: [IEEE-CIS Fraud Detection](https://www.kaggle.com/c/ieee-fraud-detection)

---