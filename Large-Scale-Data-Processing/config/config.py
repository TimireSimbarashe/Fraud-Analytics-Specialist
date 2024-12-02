import os
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType


EXPECTED_SCHEMA = StructType(
    [
        StructField("Time", IntegerType(), True),
        StructField("V1", DoubleType(), True),
        StructField("V2", DoubleType(), True),
        StructField("V3", DoubleType(), True),
        StructField("V4", DoubleType(), True),
        StructField("V5", DoubleType(), True),
        StructField("V6", DoubleType(), True),
        StructField("V7", DoubleType(), True),
        StructField("V8", DoubleType(), True),
        StructField("V9", DoubleType(), True),
        StructField("V10", DoubleType(), True),
        StructField("V11", DoubleType(), True),
        StructField("V12", DoubleType(), True),
        StructField("V13", DoubleType(), True),
        StructField("V14", DoubleType(), True),
        StructField("V15", DoubleType(), True),
        StructField("V16", DoubleType(), True),
        StructField("V17", DoubleType(), True),
        StructField("V18", DoubleType(), True),
        StructField("V19", DoubleType(), True),
        StructField("V20", DoubleType(), True),
        StructField("V21", DoubleType(), True),
        StructField("V22", DoubleType(), True),
        StructField("V23", DoubleType(), True),
        StructField("V24", DoubleType(), True),
        StructField("V25", DoubleType(), True),
        StructField("V26", DoubleType(), True),
        StructField("V27", DoubleType(), True),
        StructField("V28", DoubleType(), True),
        StructField("Amount", DoubleType(), True),
        StructField("Class", IntegerType(), True),
    ]
)

RAW_DATA_PATH = os.path.join("data", "raw", "creditcard.csv")
PROCESSED_DATA_PATH = os.path.join("data", "processed", "creditcard_processed")
TRANSFORMED_DATA_PATH = os.path.join("data", "transformed", "creditcard_transformed")
REPORTS_PATH = os.path.join("docs", "reports")


SPARK_CONFIG = {
    "spark.app.name": "CreditCardFraudDetection",
    "spark.master": "local[*]",
    "spark.executor.memory": "4g",
    "spark.executor.cores": "4",
    "spark.num.executors": "50",
    "spark.driver.memory": "4g",
    "spark.driver.cores": "4",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.autoBroadcastJoinThreshold": "10485760",  # 10 MB
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.persistSortedMergeJoin": "true",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.network.timeout": "600s",
}

KAGGLE_DATASET = "mlg-ulb/creditcardfraud"
KAGGLE_FILENAME = "creditcard.csv"