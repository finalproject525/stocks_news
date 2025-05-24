import os
import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import API_TOKEN_FINNHUB, SYMBOLS_TEST, TOPIC, FROM_DATE, TO_DATE, BROKER, BASE_URL_FINNHUB
from kafka import KafkaProducer

# -------------------- Debug Logging --------------------
logging.basicConfig(level=logging.DEBUG)


def main():
    SYMBOLS = tuple(SYMBOLS_TEST)
    global TOPIC
    if TOPIC != 'finnhub-data':
        TOPIC = 'finnhub-data'

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

    KAFKA_BOOTSTRAP_SERVERS = BROKER[0]

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 8)
    )

    results = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_request = {}

        for symbol in SYMBOLS:
            for endpoint_name, endpoint_info in ENDPOINTS.items():
                url = f"{BASE_URL_FINNHUB}/{endpoint_info['url']}"
                params = {
                    'symbol': symbol,
                    'token': API_TOKEN_FINNHUB
                }
                params.update(endpoint_info['params'])

                def task(url=url, params=params, symbol=symbol, endpoint_name=endpoint_name):
                    try:
                        response = requests.get(url, params=params, timeout=10)
                        response.raise_for_status()
                        data = response.json()

                        payload = {
                            'symbol': symbol,
                            'endpoint': endpoint_name,
                            'data': data
                        }

                        producer.send(TOPIC, value=payload)

                        return {
                            'symbol': symbol,
                            'endpoint': endpoint_name,
                            'status': 'sent_to_kafka'
                        }

                    except requests.RequestException as e:
                        return {
                            'symbol': symbol,
                            'endpoint': endpoint_name,
                            'error': str(e)
                        }

                future = executor.submit(task)
                future_to_request[future] = (symbol, endpoint_name)

        for future in as_completed(future_to_request):
            result = future.result()
            results.append(result)

    for result in results:
        if 'error' in result:
            print(f"❌ Error for {result['symbol']} - {result['endpoint']}: {result['error']}")
        else:
            print(f"✅ Sent {result['symbol']} - {result['endpoint']} to Kafka")

    producer.flush()
    producer.close()

# -------------------- Run --------------------

if __name__ == "__main__":
    main()