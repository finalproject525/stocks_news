import os
import sys

# Set project root in PYTHONPATH
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# ✅ Set this to True for yfinance, False for finnhub
use_cons_yahoo = False

# ✅ Shared Kafka/MinIO config
os.environ["USE_MINIO"] = "True"
os.environ["MINIO_ROOT_USER"] = "minioadmin"
os.environ["MINIO_ROOT_PASSWORD"] = "minioadmin"
os.environ["MINIO_ENDPOINT"] = "http://minio_spg:9000"
os.environ["OUTPUT_FORMATS"] = "json,parquet"
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "kafka_spg:9092"

# ✅ Topic-specific config
if use_cons_yahoo:
    os.environ["TOPIC"] = "yfinance-data"
    os.environ["SCHEMA"] = "yfinance"
    print("🟡 Running YFinance Consumer (schema_yfinance)")
else:
    os.environ["TOPIC"] = "finnhub-data"
    os.environ["SCHEMA"] = "finnhub"  # if needed
    os.environ["FROM_DATE"] = "2024-01-01"
    os.environ["TO_DATE"] = "2025-05-22"
    print("🔵 Running Finnhub Consumer (dynamic endpoint schema)")

# ✅ Launch main consumer
from consumer.main_consumer import main

if __name__ == "__main__":
    main()
