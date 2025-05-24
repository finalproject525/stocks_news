
# import requests

# url = "https://finnhub.io/api/v1//company-news?symbol=AAPL&from=2024-01-15&to=2025-02-20"
# params = {
#     'symbol': 'AAPL',
#     'token': 'd0h0tk1r01qv1u34jdggd0h0tk1r01qv1u34jdh0'
# }

# response = requests.get(url, params=params)
# data = response.json()
# print(data)


#Importing Liberaries
import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Defining API parameters
API_TOKEN = 'd0h0tk1r01qv1u34jdggd0h0tk1r01qv1u34jdh0'
SYMBOLS = ["AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "META", "AMZN"]
FROM_DATE = '2024-01-15'
TO_DATE = '2025-02-20'
BASE_URL = "https://finnhub.io/api/v1"
OUTPUT_DIR = "finnhub_data"

# Checking if folder exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Adding more parameters for the endpoints (if needed)
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
    "usa-spending": {
        "url": "stock/usa-spending",
        "params": {}
    }
}

#Function for url string - by endpoint and symbol
def fetch_data(symbol, endpoint_name, endpoint_info):
    url = f"{BASE_URL}/{endpoint_info['url']}"
    params = {
        'symbol': symbol,
        'token': API_TOKEN
    }
    params.update(endpoint_info['params'])

#exceptions for monitoring
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # שמירה לקובץ JSON
        filename = f"{symbol}_{endpoint_name}.json"
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return {
            'symbol': symbol,
            'endpoint': endpoint_name,
            'status': 'saved',
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

    # Printing for monitoring
    for result in results:
        if 'error' in result:
            print(f"❌ Error for {result['symbol']} - {result['endpoint']}: {result['error']}")
        else:
            print(f"✅ Saved {result['symbol']} - {result['endpoint']} to {result['file']}")
            return result
if __name__ == "__main__":
    main()
