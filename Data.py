"""
This file 

The repository can be found at https://github.com/man-c/pycoingecko/
The documentation of the CoinGecko API can be found at https://www.coingecko.com/en/api/documentation
"""

import os
import time
import datetime

# downloading the CoinGecko Python API
# os.system("python3 -m pip install -U pycoingecko")

from pycoingecko import CoinGeckoAPI
cg = CoinGeckoAPI()
# if you have a Pro API Key run 'cg = pycoingecko.CoinGeckoAPI(api_key='YOUR_API_KEY')'

# retrieving a list of all coin IDs
# the initial API call returns a list of dictionaries with detailed information
ids_list = cg.get_coins_list()

ids = []
for coin in ids_list:
    ids.append(coin["id"])

# checking for duplicate IDs
for id in ids:
    if ids.count(id) > 1:
        print("This ID occurs more than once: " + id)

# Creating Unix timestamps
# the end_date is today
start_date = time.mktime((2010, 1, 1, 12, 0, 0, 4, 1, -1))
# info regarding the arguments can be found at https://docs.python.org/3/library/time.html#time.struct_time
end_date = time.time()

# checking if the Unix dates are correct
print(datetime.datetime.fromtimestamp(start_date))
print(datetime.datetime.fromtimestamp(end_date))

# filling a dictionary with historic data for each coin
# this might take a while
historic_data = {}
for id in ids[0:5]:
    historic_data[id] = cg.get_coin_market_chart_range_by_id(id=id, vs_currency="usd", from_timestamp=start_date, to_timestamp=end_date)["prices"]
    # the function returns a dictionary with only one entry
    # converting it into a list where every entry is a timestamp with the respective price in USD
    # 

for i in historic_data.values():
    print(len(i))
