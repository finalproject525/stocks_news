import os
from datetime import datetime
#######################TEST VARIABLES##########################

# If False, fetch all 500 tickers from Wikipedia
USE_SYMBOLES_TEST = True
SYMBOLS_TEST = ("AAPL", "MSFT", "GOOG","TSLA","NVDA","META","AMZN")




################ yfinance api config#############################
# Valid values for yfinance `period`
# "1d", "5d", "7d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"

# Valid values for yfinance `interval`
# "1m", "2m", "5m", "15m", "30m", "60m", "90m", "1d", "5d", "1wk", "1mo", "3mo"

# Max period allowed for each interval
# "1m", "2m", "5m", "15m", "30m", "60m", "90m"  => max period: "7d"
# "1d", "5d", "1wk", "1mo", "3mo"              => max period: "max"
PERIOD = "300d"
INTERVAL = "60m" 
YFINANCE_BATCH_SIZE  = 50

################ finnhub api config#############################
#API_TOKEN_FINNHUB = 'd0h0tk1r01qv1u34jdggd0h0tk1r01qv1u34jdh0'
API_TOKEN_FINNHUB = 'd0gtgm9r01qhao4vr03gd0gtgm9r01qhao4vr040'
FROM_DATE = os.getenv('FROM_DATE', '2024-01-15')
TO_DATE = os.getenv('TO_DATE', datetime.today().strftime('%Y-%m-%d'))

BASE_URL_FINNHUB = "https://finnhub.io/api/v1"



################ Kafka Config ###################################
TOPIC = 'yfinance-data'

BROKER = ['kafka_spg:9092']
KAFKA_BATCH_SIZE  = 100 

##################Spark#####################################
SCHEMA_DEFAULT='yfinance'

################### AWS #########################################

OUTPUT_FORMATS = ['json']#, 'parquet']

S3_ACCESS = {
    "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
    "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "endpoint": f"s3.{os.getenv('AWS_DEFAULT_REGION')}.amazonaws.com"
}
BUCKET = "final-de-project-sp500" 

#############MINIO#######################

# If True, use MinIO; if False, use AWS S3
USE_MINIO = True

MINIO_ACCESS= {
    "access_key": os.getenv("MINIO_ROOT_USER"),
    "secret_key": os.getenv("MINIO_ROOT_PASSWORD"),
    "endpoint": os.getenv('MINIO_ENDPOINT')  or "minio_spg:9000"
}





