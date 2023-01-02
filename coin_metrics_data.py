"""
https://coinmetrics.github.io/api-client-python/site/index.html

https://docs.coinmetrics.io/api/v4

"""

# import os
# pip3 install coinmetrics-api-client

import pandas as pd, datetime
from coinmetrics.api_client import CoinMetricsClient

client = CoinMetricsClient()

print(client.catalog_assets())

metrics = client.get_asset_metrics(assets="btc", metrics=["PriceUSD", "AdrActCnt", "CapMrktCurUSD", "CapMVRVCur"], start_time="2018-01-01", end_time="2022-01-01", frequency="1d")
metrics = pd.DataFrame(metrics)
metrics["time"] = datetime.date(metrics["time"])
print(metrics.head())
