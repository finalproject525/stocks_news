# main.py
from ETL.pipeline.runner import ETLRunner
import os
import fsspec
from config import MINIO_ACCESS,BUCKET


import fsspec
from urllib.parse import urlparse
import socket

#watermark_path = "s3a://final-de-project-sp500/curated/etl_state/ohlcv_latest.json"


def main():

    raw_path = f"s3a://{BUCKET}/raw"
    curated_path = f"s3a://{BUCKET}/curated"
    state_dir = f"{curated_path}/etl_state"
    runner = ETLRunner(
        raw_dir=raw_path,
        curated_dir=curated_path,
        state_dir=state_dir,
        storage_options = {
                "key": MINIO_ACCESS['access_key'],
                "key": MINIO_ACCESS['secret_key'],
                "key": MINIO_ACCESS['endpoint']


        }

    )

    runner.run




















def resolve_hostname_to_ip(hostname):
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return hostname  # fallback

def get_fs_from_storage_config(storage_access: dict):
    raw_endpoint = storage_access.get("endpoint", "").strip()
    parsed = urlparse(raw_endpoint)
    ip = resolve_hostname_to_ip(parsed.hostname)
    resolved_endpoint = f"{parsed.scheme}://{ip}:{parsed.port}"

    return fsspec.filesystem(
        "s3",
        key=storage_access.get("access_key"),
        secret=storage_access.get("secret_key"),
        client_kwargs={"endpoint_url": resolved_endpoint}
    )



if __name__ == "__main__":
    main()