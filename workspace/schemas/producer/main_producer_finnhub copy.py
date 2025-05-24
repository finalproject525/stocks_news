import os
import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer

# -------------------- Debug Logging --------------------
logging.basicConfig(level=logging.DEBUG)

# -------------------- Configuration --------------------

# API Finnhub
API_TOKEN = 'd0h0tk1r01qv1u34jdggd0h0tk1r01qv1u34jdh0'
SYMBOLS = ["AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "META", "AMZN"]
FROM_DATE = '2024-01-15'
TO_DATE = '2025-02-20'
BASE_URL = "https://finnhub.io/api/v1"

# Output directory
OUTPUT_DIR = "finnhub_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Endpoints
ENDPOINTS = {
    "company-profile": {
        "url": "stock/profile2",
        "params": {}
    },
    "company-news": {
        "url": "company-news",
        "params": {"from": FROM_DATE, "to": TO_DATE}
    },
    "insider-transactions": {
        "url": "stock/insider-transactions",
        "params": {}
    },
    "insider-sentiment": {
        "url": "stock/insider-sentiment",
        "params": {}
    },
    "uspto-patents": {
        "url": "stock/uspto-patent",
        "params": {}
    },
    "usa-spending": {
        "url": "stock/usa-spending",
        "params": {}
    },
    "earning-surprises": {
        "url": "stock/earnings-surprises",
        "params": {}
    }
}

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka_spg:9092'
KAFKA_TOPIC = 'finnhub-data'

# Kafka producer with fixed API version
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 8)
)

# -------------------- Main Logic --------------------

def fetch_data(symbol, endpoint_name, endpoint_info):
    url = f"{BASE_URL}/{endpoint_info['url']}"
    params = {
        'symbol': symbol,
        'token': API_TOKEN
    }
    params.update(endpoint_info['params'])

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Build Kafka message
        payload = {
            'symbol': symbol,
            'endpoint': endpoint_name,
            'data': data
        }

        # Send to Kafka
        producer.send(KAFKA_TOPIC, value=payload)

        # Save to file
        filename = f"{symbol}_{endpoint_name}.json"
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return {
            'symbol': symbol,
            'endpoint': endpoint_name,
            'status': 'sent_to_kafka',
            'file': filepath
        }

    except requests.RequestException as e:
        return {
            'symbol': symbol,
            'endpoint': endpoint_name,
            'error': str(e)
        }

def main():
    results = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_request = {
            executor.submit(fetch_data, symbol, endpoint, info): (symbol, endpoint)
            for symbol in SYMBOLS
            for endpoint, info in ENDPOINTS.items()
        }

        for future in as_completed(future_to_request):
            result = future.result()
            results.append(result)

    for result in results:
        if 'error' in result:
            print(f"❌ Error for {result['symbol']} - {result['endpoint']}: {result['error']}")
        else:
            print(f"✅ Sent {result['symbol']} - {result['endpoint']} to Kafka and saved to {result['file']}")

    producer.flush()
    producer.close()

# -------------------- Run --------------------

if __name__ == "__main__":
    main()
