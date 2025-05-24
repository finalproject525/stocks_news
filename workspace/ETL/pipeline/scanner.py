# pipeline/scanner.py

import fsspec
from urllib.parse import urlparse
import socket
import datetime

class PartitionScanner:
    """
    Scans raw storage to:
      - list all datasets (i.e. logical tables / endpoints)
      - list all hourly partitions newer than a given watermark
    """

    def __init__(self, raw_dir: str, storage_options: dict):
        self.raw_dir = raw_dir.rstrip("/")
        self.storage_options = storage_options
        self.fs = self._init_fs()

    def _init_fs(self):
        """
        Initialize a fsspec S3/MinIO filesystem.
        """
        # resolve endpoint host → IP etc...
        parsed = urlparse(self.storage_options["endpoint"])
        # ... same logic as in your main_ETL
        endpoint = f"{parsed.scheme}://{socket.gethostbyname(parsed.hostname)}:{parsed.port}"
        return fsspec.filesystem(
            "s3",
            key=self.storage_options["access_key"],
            secret=self.storage_options["secret_key"],
            client_kwargs={"endpoint_url": endpoint}
        )

    def list_datasets(self) -> list[str]:
        """
        Return the list of known datasets under raw_dir.
        You can either hard‐code them:
        """
        return [
            "yfinance-data/ohlcv",
            "finnhub-data/company-profile",
            "finnhub-data/company-news",
            "finnhub-data/insider-sentiment",
            "finnhub-data/insider-transactions",
            "finnhub-data/usa-spending"
        ]



    def list_partitions(self, dataset: str, last_ts: str) -> list[str]:
        """
        List all hourly partitions for `dataset` newer than `last_ts`.
        Each partition path must look like:
          s3a://bucket/raw/<dataset>/year=YYYY/month=MM/day=DD/hour=HH
        """

        base = f"{self.raw_dir.rstrip('/')}/{dataset}"

        entries = self.fs.ls(base, detail=True)

        if last_ts:
            watermark_dt = datetime.fromisoformat(last_ts)
        else:
            watermark_dt = datetime.min  

        partitions = []
        for entry in entries:
            path = entry["name"] 

            parts = path.split("/")[-4:]  # ['year=2025','month=05','day=23','hour=17']
            d = {k: int(v) for k,v in (p.split("=") for p in parts)}
            part_dt = datetime(year=d["year"], month=d["month"], day=d["day"], hour=d["hour"])


            if part_dt > watermark_dt:
                partitions.append(path)


        partitions.sort()
        return partitions
