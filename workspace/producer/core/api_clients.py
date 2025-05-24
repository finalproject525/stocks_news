import requests
from finance.YahooFinance import YahooBatchFinanceClient

class YahooFinanceClient:
    """
    Client that retrieves stock market data from Yahoo Finance
    and yields it in batches.
    """
    def __init__(self, symbols, batch_size, period, interval):
        self.symbols = symbols
        self.batch_size = batch_size
        self.period = period
        self.interval = interval

    def fetch_batches(self):
        """
        Yield batches of market data as list of dictionaries.
        """
        client = YahooBatchFinanceClient(
            symbols=self.symbols,
            batch_size=self.batch_size,
            period=self.period,
            interval=self.interval
        )

        for batch in client._chunk_list(self.symbols, self.batch_size):
            client_batch = YahooBatchFinanceClient(
                symbols=batch,
                batch_size=self.batch_size,
                period=self.period,
                interval=self.interval
            )
            client_batch.fetch_all()
            yield client_batch.to_dict_records()



class FinnhubClient:
    """
    Client that retrieves stock market data from Finnhub and yields it in batches.
    Each batch contains multiple endpoints for a given symbol.
    """
    def __init__(self, symbols, api_token, base_url, endpoints):
        self.symbols = symbols
        self.api_token = api_token
        self.base_url = base_url
        self.endpoints = endpoints

    def fetch_batches(self):

        for symbol in self.symbols:
            batch = []
            for endpoint_name, endpoint_info in self.endpoints.items():
                url = f"{self.base_url}/{endpoint_info['url']}"
                params = {
                    'symbol': symbol,
                    'token': self.api_token
                }
                params.update(endpoint_info['params'])

                try:
                    response = requests.get(url, params=params, timeout=10)
                    response.raise_for_status()

                    data = response.json()
         
                    batch.extend(self._normalize_response(data, symbol, endpoint_name)) 

                except requests.RequestException as e:
                    batch.append({
                        'symbol': symbol,
                        'endpoint': endpoint_name,
                        'error': str(e)
                    })
            yield batch
    def _normalize_response(self, data, symbol, endpoint_name):
        messages = []

        # Case 1 – 'data' key contains a list of entries
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            for entry in data["data"]:
                messages.append({
                    "symbol": symbol,
                    "endpoint": endpoint_name,
                    **entry  # entry can have its own 'symbol' or 'endpoint', but we overwrite it
                })

        # Case 2 – top-level is a list directly
        elif isinstance(data, list):
            for entry in data:
                messages.append({
                    "symbol": symbol,
                    "endpoint": endpoint_name,
                    **entry
                })

        # Case 3 – 'data' is a single object
        elif isinstance(data, dict) and isinstance(data.get("data"), dict):
            messages.append({
                "symbol": symbol,
                "endpoint": endpoint_name,
                **data["data"]
            })

        # Case 4 – already flat
        elif isinstance(data, dict):
            messages.append({
                "symbol": symbol,
                "endpoint": endpoint_name,
                **data
            })

        return messages
