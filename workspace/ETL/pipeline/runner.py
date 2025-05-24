# pipeline/etl_runner.py
"""
ETLRunner orchestrates the full ETL: scanning, transforming, writing, and updating watermarks.
"""
import fsspec
from pyspark.sql import SparkSession
from ETL.pipeline.watermark import WatermarkStore
from ETL.pipeline.scanner import PartitionScanner
from ETL.pipeline.transformer import DatasetTransformer
from ETL.pipeline.writer import DatasetWriter


class ETLRunner:
    """
    Orchestrator for the ETL pipeline.
    """
    def __init__(
        self,
        raw_dir: str,
        curated_dir: str,
        watermark_dir: str,
        storage_options: dict
    ):
        """
        raw_dir: path to raw data
        curated_dir: path to curated data
        watermark_dir: path for watermark files
        storage_options: dict for fsspec filesystem
        """
        # Initialize filesystem (e.g., MinIO/S3)
        self.fs = fsspec.filesystem("s3", **storage_options)
        self.raw_dir = raw_dir.rstrip('/')
        self.curated_dir = curated_dir.rstrip('/')

        # Initialize components
        self.watermark_store = WatermarkStore(self.fs, watermark_dir)
        self.scanner = PartitionScanner(self.fs, raw_dir)
        self.spark = SparkSession.builder.appName("ETLRunner").getOrCreate()
        self.transformer = DatasetTransformer(self.spark)
        self.writer = DatasetWriter(self.fs, curated_dir)

    def run(self):
        """
        Execute ETL for all configured datasets.
        """
        datasets = [
            "yfinance-data/ohlcv",
            "finnhub-data/company-profile",
            "finnhub-data/company-news",
            "finnhub-data/earning-surprises",
            "finnhub-data/insider-sentiment",
            "finnhub-data/insider-transactions",
            "finnhub-data/usa-spending",
            "finnhub-data/uspto-patents"
        ]

        for dataset in datasets:
            # Read last watermark
            last_ts = self.watermark_store.read(dataset)

            # Find new partitions
            partitions = self.scanner.list_partitions(dataset, last_ts)
            for partition_path in partitions:
                # Transform raw data
                df = self.transformer.transform(dataset, partition_path)

                # Extract partition values from path (e.g., year, month, day, hour)
                parts = partition_path.split('/')[-4:]
                pv = dict(p.split('=') for p in parts)

                # Write to curated storage
                self.writer.write(dataset, df, pv)

                # Update watermark to latest partition hour
                new_ts = f"{pv['year']}-{pv['month']}-{pv['day']}T{pv['hour']}:00:00"
                self.watermark_store.write(dataset, new_ts)

        # Stop Spark session
        self.spark.stop()
