from config import FROM_DATE, TO_DATE

company_profile = {
    "url": "stock/profile2",
    "params": {}
}

company_news = {
    "url": "company-news",
    "params": {"from": FROM_DATE, "to": TO_DATE}
}
insider_transactions = {
    "url": "stock/insider-transactions",
    "params": {}
}

insider_sentiment = {
    "url": "stock/insider-sentiment",
    "params": {}
}
# uspto_patents = {
#     "url": "stock/uspto-patent",
#     "params": {}
# }
usa_spending = {
    "url": "stock/usa-spending",
    "params": {}
}
# earning_surprises = {
#     "url": "stock/earnings-surprises",
#     "params": {}
# }
FINNHUB_ENDPOINTS = {
    "company-profile": company_profile,
    "company-news": company_news,
    "insider-transactions": insider_transactions,
    "insider-sentiment": insider_sentiment,
    "usa-spending": usa_spending
}

