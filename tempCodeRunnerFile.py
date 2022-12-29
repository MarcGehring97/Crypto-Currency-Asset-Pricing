"""
This file 

The repository can be found at https://github.com/man-c/pycoingecko/
The documentation of the CoinGecko API can be found at https://www.coingecko.com/en/api/documentation
"""

import os
import time
import datetime

# downloading the CoinGecko Python API
#! os.system("python3 -m pip install -U pycoingecko")

from pycoingecko import CoinGeckoAPI
cg = CoinGeckoAPI()
# if you have a Pro API Key run 'cg = pycoingecko.CoinGeckoAPI(api_key='YOUR_API_KEY')'

# checking whether the API works
#! print(cg.ping())

# retrieving a list of all coin IDs
# the initial API call returns a list of dictionaries with detailed information
ids_list = cg.get_coins_list()

list_length = len(ids_list)
counter = 0
percentage_counter = 1
ids = []
print("The progress is: ")
for id in ids_list:
    # printing the progress 
    counter += 1
    progress = round(counter / list_length * 100, 0)
    if progress >= percentage_counter:
        percentage_counter += 1
        print(str(progress) + "%")
    
    """
    try:
        # filtering out coins with a market capitalization of less than 1m USD
        if cg.get_coin_by_id(id["id"])["market_data"]["market_cap"]["usd"] >= 1000000:
            # this might take a while
            ids.append(id["id"])
    except:
        print("There is a missing key for " + id["id"]+ " but the process continues")
    """

print(len(ids))

# checking for duplicate IDs
for id in ids:
    if ids.count(id) > 1:
        print("This ID occurs more than once: " + id)

# Creating Unix timestamps
# the end_date is today
start_date = time.mktime((2011, 1, 1, 12, 0, 0, 4, 1, -1))
# info regarding the arguments can be found at https://docs.python.org/3/library/time.html#time.struct_time
end_date = time.time()

# checking if the Unix dates are correct
print(datetime.datetime.fromtimestamp(start_date))
print(datetime.datetime.fromtimestamp(end_date))

# filling a dictionary with historic data (price, market capitalization, and total volume) for each coin
# this might take a while
historic_data = {}
for id in ids[0:1]:
    historic_data[id] = cg.get_coin_market_chart_range_by_id(id=id, vs_currency="usd", from_timestamp=start_date, to_timestamp=end_date)["prices"]
    # the function returns a dictionary with only one entry
    # converting it into a list where every entry is a timestamp with the respective price in USD
    # the data series are of different lengths depending on the availability of historic data

