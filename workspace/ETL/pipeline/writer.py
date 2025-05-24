# pipeline/writer.py
"""
DatasetWriter writes Spark DataFrames to curated storage in Parquet format.
"""
from pyspark.sql import DataFrame
from typing import Dict


class DatasetWriter:

    def __init__(self, fs, curated_base_path: str):

        self.fs = fs
        self.curated_base = curated_base_path.rstrip('/')

    def write(self, dataset: str, df: DataFrame, partitions: Dict[str, str]):
        partition_cols = [f"{k}={v}" for k, v in partitions.items()]
        path = f"{self.curated_base}/{dataset}/{'/'.join(partition_cols)}"
        df.write.mode("append").parquet(path)
