#from pyspark.sql.types import DateType,T.StructType, T.StructField, DoubleType, T.StringType, TimestampType,LongType,IntegerType
#from pyspark.sql.types import T.StructType, T.StructField, DoubleType, T.StringType, TimestampType,LongType,IntegerType
from pyspark.sql import types as T #T.DateType,T.StructType, T.StructField, DoubleType, T.StringType, TimestampType,LongType,IntegerType

schema_yfinance = T.StructType([
    T.StructField("Open", T.DoubleType(), True),
    T.StructField("High",  T.DoubleType(), True),
    T.StructField("Low",  T.DoubleType(), True),
    T.StructField("Close",  T.DoubleType(), True),
    T.StructField("Volume",  T.DoubleType(), True),
    T.StructField("symbol",  T.StringType(), True),
    T.StructField("timestamp",  T.TimestampType(), True),
    T.StructField("datetime",  T.StringType(), True)
])

# --------------------------
# 📘 1. Company Profile
# Endpoint: /stock/profile2
# --------------------------
schema_company_profile = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("endpoint", T.StringType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("country", T.StringType(), True),
    T.StructField("currency", T.StringType(), True),
    T.StructField("exchange", T.StringType(), True),
    T.StructField("finnhubIndustry", T.StringType(), True),
    T.StructField("ipo", T.DateType(), True),
    T.StructField("marketCapitalization", T.DoubleType(), True),
    T.StructField("shareOutstanding", T.DoubleType(), True)
])

# --------------------------v
# 📰 2. Company News
# Endpoint: /company-news
# --------------------------
schema_company_news = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("endpoint", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("datetime", T.LongType(), True),  # Timestamp Unix en secondes
    T.StructField("headline", T.StringType(), True),
    T.StructField("related", T.StringType(), True),
    T.StructField("summary", T.StringType(), True)
])

# --------------------------
# 🔍 3. Insider Transactions
# Endpoint: /stock/insider-transactions
# --------------------------
schema_insider_transactions = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("endpoint", T.StringType(), True),
    T.StructField("change", T.LongType(), True),
    T.StructField("transactionDate", T.StringType(), True),
    T.StructField("transactionCode", T.StringType(), True),
    T.StructField("transactionPrice", T.StringType(), True),
    T.StructField("shares", T.StringType(), True),
    T.StructField("name", T.StringType(), True)
])

# --------------------------
# 📈 4. Insider Sentiment
# Endpoint: /stock/insider-sentiment
# --------------------------
schema_insider_sentiment = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("endpoint", T.StringType(), True),
    T.StructField("year", T.IntegerType(), True),
    T.StructField("month", T.IntegerType(), True),
    T.StructField("mspr", T.DoubleType(), True)
])

# --------------------------
# 📄 5. USPTO Patents
# Endpoint: /stock/uspto-patent
# --------------------------
# schema_uspto_patents = T.StructType([
#     T.StructField("symbol", T.StringType(), True),
#     T.StructField("endpoint", T.StringType(), True),
#     T.StructField("patentNumber", T.StringType(), True),
#     T.StructField("publicationDate", T.StringType(), True),
#     T.StructField("filingStatus", T.StringType(), True),
#     T.StructField("description", T.StringType(), True)
# ])

# --------------------------
# 💵 6. USA Spending
# Endpoint: /stock/usa-spending
# --------------------------
schema_usa_spending = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("endpoint", T.StringType(), True),
    T.StructField("recipientParentName", T.StringType(), True),
    T.StructField("country", T.StringType(), True),
    T.StructField("totalValue", T.DoubleType(), True),
    T.StructField("actionDate", T.DateType(), True),
    T.StructField("performanceStartDate", T.DateType(), True),
    T.StructField("performanceEndDate", T.DateType(), True)
])
# --------------------------
# 📊 7. Earning Surprises
# Endpoint: /stock/earnings-surprises
# --------------------------

schema_earning_surprises = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("endpoint", T.StringType(), True),
    T.StructField("actual", T.DoubleType(), True),
    T.StructField("estimate", T.DoubleType(), True),
    T.StructField("surprise", T.DoubleType(), True),
    T.StructField("quarter", T.StringType(), True),
    T.StructField("error", T.StringType(), True)
])
 


ENDPOINT_SCHEMAS = {
    "company-profile": schema_company_profile,
    "company-news": schema_company_news,
    "insider-transactions": schema_insider_transactions,
    "insider-sentiment": schema_insider_sentiment,
    "usa-spending": schema_usa_spending,
    "earning-surprises": schema_earning_surprises
}

# --------------------------
# 📘 1. Company Profile
# Endpoint: /stock/profile2
# --------------------------
""" schema_company_profile = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("endpoint", T.StringType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("country", T.StringType(), True),
    T.StructField("industry", T.StringType(), True),
    T.StructField("ticker", T.StringType(), True)
])

 """


# --------------------------
# 💵 6. USA Spending
# Endpoint: /stock/usa-spending
# --------------------------
""" schema_usa_spending = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("endpoint", T.StringType(), True),
    T.StructField("contractName", T.StringType(), True),
    T.StructField("amount", DoubleType(), True),
    T.StructField("date", T.StringType(), True)
]) """


# --------------------------
# 📰 2. Company News
# Endpoint: /company-news
# --------------------------
""" schema_company_news = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("endpoint", T.StringType(), True),
    T.StructField("headline", T.StringType(), True),
    T.StructField("datetime", T.StringType(), True),  # Unix timestamp
    T.StructField("source", T.StringType(), True)
])
 """
# --------------------------
# 🔍 3. Insider Transactions
# Endpoint: /stock/insider-transactions
# --------------------------
""" schema_insider_transactions = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("endpoint", T.StringType(), True),
    T.StructField("transactionDate", T.StringType(), True),
    T.StructField("transactionCode", T.StringType(), True),
    T.StructField("transactionPrice", T.StringType(), True),
    T.StructField("shares", T.StringType(), True),
    T.StructField("name", T.StringType(), True)
]) """

""" 
# --------------------------
# 📈 4. Insider Sentiment
# Endpoint: /stock/insider-sentiment
# --------------------------
schema_insider_sentiment = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("endpoint", T.StringType(), True),
    T.StructField("year", T.IntegerType(), True),
    T.StructField("month", T.IntegerType(), True),
    T.StructField("mspr", DoubleType(), True)
])
 """