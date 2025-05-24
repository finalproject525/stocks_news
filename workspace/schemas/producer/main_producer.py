import os
import queue
from producer.core import YahooFinanceClient, ProducerManager, FinanceFetcher, KafkaSender ,FinnhubClient
from schemas.finnhub_endpoints_schemas import FINNHUB_ENDPOINTS
from config import USE_SYMBOLES_TEST, SYMBOLS_TEST, BROKER, TOPIC, PERIOD, INTERVAL, YFINANCE_BATCH_SIZE,API_TOKEN_FINNHUB, BASE_URL_FINNHUB
from finance.functions import get_sp500_symbol
 
def main():
    """
    Main entrypoint: reads environment variables and sets up the pipeline.
    Designed to work inside a Docker container triggered by Airflow.
    """  
 

    # Lire les paramètres depuis les variables d'environnement
    use_symbols_test = os.getenv("USE_SYMBOLS_TEST", str(USE_SYMBOLES_TEST)).lower() in ["true", "1", "yes"] 
    symbols_test = SYMBOLS_TEST
    broker = os.getenv("BROKER", BROKER)
    topic = os.getenv("TOPIC", TOPIC)
    period = os.getenv("PERIOD", PERIOD)
    interval = os.getenv("INTERVAL", INTERVAL)
    batch_size = int(os.getenv("BATCH_SIZE", YFINANCE_BATCH_SIZE))
    api_name = os.getenv("API_NAME", "yfinance")
 


    # Choix des symboles
    symbols = symbols_test if use_symbols_test else get_sp500_symbol()['Symbol'].to_list()
 
    # Initialisation de la chaîne de traitement
    data_queue = queue.Queue()


    if api_name == "yfinance":
        client = YahooFinanceClient(symbols, batch_size, period, interval)
        partition_key = 'Symbol'
    elif api_name == "finnhub":
        client = FinnhubClient(symbols, API_TOKEN_FINNHUB, BASE_URL_FINNHUB, FINNHUB_ENDPOINTS)
        partition_key = 'endpoint'
    else:
        raise NotImplementedError(f"API '{api_name}' is not supported.")

    producer = ProducerManager(broker)
    fetcher = FinanceFetcher(client, data_queue)
    sender = KafkaSender(producer, topic,partition_key, data_queue)

    fetcher.start()
    sender.start()
    fetcher.join()
    sender.join()


if __name__ == "__main__":
    main()
