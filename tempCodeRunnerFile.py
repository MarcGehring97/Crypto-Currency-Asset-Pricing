"""
This file 

The repository can be found at https://github.com/man-c/pycoingecko/
The documentation of the CoinGecko API can be found at https://www.coingecko.com/en/api/documentation
"""

import os
import time


# downloading the CoinGecko Python API
# os.system("python3 -m pip install -U pycoingecko")

from pycoingecko import CoinGeckoAPI
cg = CoinGeckoAPI()
# if you have a Pro API Key run 'cg = pycoingecko.CoinGeckoAPI(api_key='YOUR_API_KEY')'

# retrieving a list of all coin IDs => the initial API call returns a list of dictionaries with detailed information
ids_list = cg.get_coins_list()

ids = []
for coin in ids_list:
    ids.append(coin["id"])

# checking for duplicate IDs
for id in ids:
    if ids.count(id) > 1:
        print("This ID occurs more than once: " + id)

# filling a dictionary with historic data for each coin
historic_data = {}
for id in ids:
    historic_data[id] = cg.get_coin_history_by_id(id, "30-12-2017")

print(time.time())