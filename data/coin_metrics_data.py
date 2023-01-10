"""
This file returns or downloads data from https://coinmetrics.io/. Information regarding the API can be found at https://docs.coinmetrics.io/api/v4
and https://coinmetrics.github.io/api-client-python/site/index.html. An example of an API file can be found at
https://github.com/coinmetrics/api-client-python/blob/master/examples/notebooks/walkthrough_community.ipynb. A comprehensive list of the
available metrics plus the abbreviations can be found at https://docs.coinmetrics.io/info/metrics.

The function "retrieve_data" has the following arguments:
- start_date: The start date specified in the data_processing file. 
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- metrics: A list of all metrics for which the time series should be downloaded or returned. The default are the two available metrics of the
          three that are needed.
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "retrieve_data" returns a pd dataframe with columns for date, AdrActCnt and TxCnt
"""

__all__ = ["retrieve_data"]

def retrieve_data(start_date, end_date, path="", metrics=["AdrActCnt", "TxCnt"], download=True, pro_key=""):
    
    import pandas as pd, os
    from coinmetrics.api_client import CoinMetricsClient

    client = CoinMetricsClient(pro_key)

    if path != "":
        file_names = os.listdir(path)

    # "catag_assets()" returns a list of dictionaries that contain data about the coins
    # On the 2nd of January 2020 there were 3020 assets

    
    # in the paper, they use only Bitcoin network data
    metrics = client.get_asset_metrics(assets="btc", metrics=metrics, start_time=start_date, end_time=end_date, frequency="1d")
    metrics = pd.DataFrame(metrics)
    metrics.insert(0, "date", pd.to_datetime(metrics["time"]).dt.date) 
    metrics = metrics.drop(["time", "asset"], axis=1)
    
    if download:
        if "coin_metrics_data.csv" not in file_names:
            metrics.to_csv(path + "/coin_metrics_data.csv", index=False)
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/coin_metrics_data.csv")
                metrics.to_csv(path + "/coin_metrics_data.csv", index=False)
            else:
                print("Could not create a new file.")
    else: 
        return metrics

import datetime
# print(retrieve_data(start_date="2014-01-01", end_date=str(datetime.date.today()), download=False).head())