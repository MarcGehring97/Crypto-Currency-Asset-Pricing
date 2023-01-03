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


Furthermore:
- Data by the Price Monitoring Center, NDRC was not available. As it turns out, the National Bureau of Statistics of China also does not
  provide data about the average price of electricity.
- The price data for the Bitman Antminer on https://keepa.com/#!search/1-bitmain%20antminer is not available for a long enough time period.
"""

import coingecko_data as cg, blockchain_com_data as bc, coin_metrics_data as cm, us_eia_data as ue, google_trends_data as gt

path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"
path_coingecko = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data/coingecko"
charts = ["n-unique-addresses", "n-transactions"]
# charts.append("n-payments")
# the API for the chart n-payments is currently not available
metrics = ["AdrActCnt", "TxCnt"]
# keyword list
kw_list = ["Bitcoin"]

cg.retrieve_data(path=path_coingecko)
bc_data = bc.retrieve_data(charts=charts, download=False)
cm_data = cm.retrieve_data(metrics=metrics, download=False)
ue_data = ue.retrieve_data()
gt_data = gt.retrieve_data(kw_list=kw_list, download=False)

# preprocessing the US EIA data
us_eia_data = ue.retrieve_data()
# look at the URL and the tables in the browser => what about missing values and the frequency?

# discrepancy between monthly and daily data


# looking at rows where all values are not equal to 0
# interest = interest[(interest != 0).all(1)]

# drop all rows that have null values in all columns
# interest.dropna(how='all',axis=0, inplace=True)
