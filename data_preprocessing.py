"""
This file contains the steps to combine all data sets and do the preprocessing. It is especially important to consider missing dates and what
to substitute this data with.

The data comes from the following sources:
- coingecko_data: The cryptocurrency data can be retrieved from CoinGecko. Downloading all the data takes several hours if not days. Therefore,
                  it makes more sense to store the data in files and import them again in this file.
- blockchain_com_data: From this source one can get the number of unique addresses and the number of transactions. Sadly, the variable 
                       "n-payments" is not available through the API. In the paper, they only use this source for the number of wallet users
                       but this information is not available.
- coin_metrics_data: This data contains the variables "AdrActCnt" and "TxCnt" which are equivalent to the two variables from blockchain.com.
                     In the paper, they also retrieve data on the payment count. This data is not available on the website, though.
- us_eia_data: This data contains the three time series "average_price", "net_generation", and "demand". This data has to be aggregated across
               industries.
- google_trends_data: The Google Trends data for the keyword "Bitcoin". This data is available at a monthly frequency only.
- twitter_data: The data series for number of tweets containing the word "Bitcoin".

Furthermore:
- Data by the Price Monitoring Center, NDRC was not available. As it turns out, the National Bureau of Statistics of China also does not
  provide data about the average price of electricity.
- The price data for the Bitman Antminer on https://keepa.com/#!search/1-bitmain%20antminer is not available for a long enough time period.
"""

import data.coingecko_data as cg, data.blockchain_com_data as bc, data.coin_metrics_data as cm, data.us_eia_data as ue, data.google_trends_data as gt, data.twitter_data as tw, data.fred_data as fr

path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"
charts = ["n-unique-addresses", "n-transactions"]
bearer_token = ""
query = ["Bitcoin"]
# charts.append("n-payments")
# the API for the chart n-payments is currently not available
metrics = ["AdrActCnt", "TxCnt"]
# keyword list
kw_list = ["Bitcoin"]
series_ids = ["DEXUSAL", "DEXCAUS", "DEXUSEU", "DEXSIUS", "DEXUSUK"]
# the last four letters describe the two currencies

path_coingecko = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data/coingecko"
cg_data = 
# the CoinGecko data needs to be uploaded from a file
# the data contains the variables id, date, price, market_cap, and total_volume
ue_data = 
# the US Energy Information Administration data needs to be uploaded from a file
# the average price data is available only at monthly frequency => I hence used the respective value for all days in a given month
# the data contains the variables date, average_price, net_generation, demand
bc_data = bc.retrieve_data(charts=charts, download=False)
# the function "retrieve_data" returns a pd dateframe with columns for date, n-unique-addresses, and n-transactions
cm_data = cm.retrieve_data(metrics=metrics, download=False)
# the function "retrieve_data" returns a pd dataframe with columns for date, AdrActCnt and TxCnt
gt_data = gt.retrieve_data(kw_list=kw_list, download=False)
# the function "retrieve_data" returns a pd dataframe with columns for date and search_count
tw_data = tw.retrieve_data(query=query, bearer_token=bearer_token, download=False)
# the function "retrieve_data" returns a pd dataframe with columns for date and tweet_count
# the data point for today is not available => this series has hence one data point fewer
fr_data = fr.retrieve_data(series_ids=series_ids)
# the function "retrieve_data" returns a pd dataframe with columns for date, DEXUSAL, DEXCAUS, DEXUSEU, DEXSIUS, DEXUSUK

# preprocessing the US EIA data
us_eia_data = ue.retrieve_data()
# look at the URL and the tables in the browser => what about missing values and the frequency?

# discrepancy between monthly and daily data

# looking at rows where all values are not equal to 0
# interest = interest[(interest != 0).all(1)]

# drop all rows that have null values in all columns
# interest.dropna(how='all',axis=0, inplace=True)

# formatting the dates to the same format
# df = df.rename(columns={"Bitcoin": "seach_count"})