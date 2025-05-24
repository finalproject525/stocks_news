# pipeline/watermark.py
"""
WatermarkStore manages reading and writing watermarks for each dataset.
"""
import json
from typing import Optional


class WatermarkStore:
    """
    Stores and retrieves last-processed timestamps for datasets.
    """
    def __init__(self, fs, base_path: str):
        self.fs = fs
        self.base_path = base_path.rstrip('/')

    def _filepath(self, dataset: str) -> str:
        return f"{self.base_path}/{dataset}_latest.json"

    def read(self, dataset: str) -> Optional[str]:
        path = self._filepath(dataset)
        try:
            with self.fs.open(path, 'r') as f:
                data = json.load(f)
            return data.get('last_processed')
        except FileNotFoundError:
            return None

    def write(self, dataset: str, timestamp: str):
        path = self._filepath(dataset)
        data = {"last_processed": timestamp}
        with self.fs.open(path, 'w') as f:
            json.dump(data, f)
