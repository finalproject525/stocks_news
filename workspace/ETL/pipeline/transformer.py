# pipeline/transformer.py
"""
DatasetTransformer loads raw JSON partitions into Spark DataFrames and applies transformations.
"""
from pyspark.sql import SparkSession, DataFrame


class DatasetTransformer:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform(self, dataset: str, partition_path: str) -> DataFrame:
        #df = self.spark.read.json(partition_path)
        #df = df.withColumn("ingested_at", current_timestamp())
        # TODO: Add custom transformations per dataset
        return 1
