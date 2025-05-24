import sys
import os

# Déterminer la racine du projet
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

local_use_yahoo = False  # ⬅️ 
os.environ["USE_SYMBOLS_TEST"] = "True"
os.environ["BATCH_SIZE"] = "50"
os.environ["BROKER"] = "kafka_spg:9092"
os.environ["MODE"] = "dev"

if local_use_yahoo:
    print("🔁 Using Yahoo Finance configuration")
    os.environ["API_NAME"] = "yfinance"
    os.environ["TOPIC"] = "yfinance-data"
    os.environ["PERIOD"] = "1d"
    os.environ["INTERVAL"] = "60m"
    
else:
    print("🔁 Using Finnhub configuration")
    os.environ["API_NAME"] = "finnhub"
    os.environ["TOPIC"] = "finnhub-data"    
    os.environ["FROM_DATE"] = "2024-08-01"
    os.environ["TO_DATE"] = "2025-05-20"

from producer.main_producer import main

if __name__ == "__main__":
    main()
