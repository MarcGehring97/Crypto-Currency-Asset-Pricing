"""
This file can be used to download a csv file of price, market cap, and volume timeseries data for a group of 
cryptocurrencies that can be filtered by specified criteria using the free CoinGecko (https://www.coingecko.com/) 
API. The Github repository can be found at https://github.com/man-c/pycoingecko/ and the documentation of the 
CoinGecko API can be found at https://www.coingecko.com/en/api/documentation. The user should make sure that the 
required libraries are installled (pandas and pycoingecko). 
The user can also use his/her CoinGecko Pro API Key. The user may also add further filtering criteria and change 
the chosen time period. For convenience, this code allows the user to download the data set in several data 
subsets. The user may specify the size of every data subset. Finally, the user should change the variable "path" 
that determines where the file will be stored. The progress may sometimes stop due to the limit imposed upon the 
number of API calls per minute of 50. The last successfully used starting index for the while loop in the main 
function "create_data_files" is printed and also indicated at the end of the file names. If the process is 
interrupted for any reason the user can call the main function at the bottom of this file with a new starting 
index 'create_data_files(starting_index = <new starting index>)'.
"""

#! import os
#! os.system("pip3 install pandas")
#! os.system("python3 -m pip install -U pycoingecko")

import time
import datetime
import pandas as pd
from pycoingecko import CoinGeckoAPI

cg = CoinGeckoAPI()
# if you have a Pro API Key run 'cg = pycoingecko.CoinGeckoAPI(api_key='YOUR_API_KEY')'

# checking whether the API works
print(cg.ping())

# this function takes in a crude list of coin IDs and returns a filtered list of a predefined size
def filtered_ids(coin_list, starting_index, ids_per_data_subset):
    index = starting_index
    percentage_counter = 1
    ids = []
    print("The filtering progress for this data subset is: ")
    while True:
        # if the end of the whole list is reached
        if index + 1 <= len(coin_list):
            # if the number of IDs per data subset is reached
            if len(ids) < ids_per_data_subset:
                # filtering out coins with a current market capitalization of less than 1m USD
                try:
                    if cg.get_coin_by_id(coin_list[index]["id"])["market_data"]["market_cap"]["usd"] >= 1000000:
                        # this might take a while
                        ids.append(coin_list[index]["id"])
                except:
                    print("There is a missing key for " + coin_list[index]["id"]+ " but the process will continue")
                index += 1
                # printing the progress 
                progress = int((index - starting_index) / ids_per_data_subset * 100)
                if progress >= percentage_counter:
                    percentage_counter += 1
                    print(str(progress) + "%")
            else:
                break
        else:
            break
        

    # checking for duplicate IDs (there are usually none)
    for id in ids:
        if ids.count(id) > 1:
            print("This ID occurs more than once: " + id)
    
    return (ids, index)

# Creating Unix timestamps
# the end_date is today
start_date = time.mktime((2011, 1, 1, 12, 0, 0, 4, 1, -1))
# info regarding the arguments can be found at https://docs.python.org/3/library/time.html#time.struct_time
end_date = time.time()

# checking if the Unix dates are correct
# print(datetime.datetime.fromtimestamp(start_date))
# print(datetime.datetime.fromtimestamp(end_date))

# creating a data frame with the historic data (dates, prices, market capitalizations, and total volumes) for each coin ID
def retrieving_data(ids):
    # filling a dictionary with historic data (dates, prices, market capitalizations, and total volumes) for each coin ID
    historic_data = {"id": [], "dates": [], "prices": [], "market_caps": [], "total_volumes": []}
    print("The retrieval progress for this data subset is: ")
    counter = 0
    percentage_counter = 1
    list_length = len(ids)
    for id in ids:
        # printing the progress 
        counter += 1
        progress = int(counter / list_length * 100)
        if progress >= percentage_counter:
            percentage_counter += 1
            print(str(progress) + "%")
        
        # retrieving the data 
        data = cg.get_coin_market_chart_range_by_id(id=id, vs_currency="usd", from_timestamp=start_date, to_timestamp=end_date)
        # this might take a while
        historic_data["id"].append(id)

        dates = []
        prices = []
        for i in data["prices"]:
            dates.append(i[0])
            dates.append(i[1])
        historic_data["dates"].append(dates)
        historic_data["prices"].append(prices)

        market_caps = []
        for i in data["market_caps"]:
            market_caps.append(i[1])
        historic_data["market_caps"].append(market_caps)

        total_volumes = []
        for i in data["total_volumes"]:
            market_caps.append(i[1])
        historic_data["total_volumes"].append(total_volumes)

    historic_data = pd.DataFrame.from_dict(historic_data)
    # the data series are of different lengths depending on the availability of historic data
    return historic_data

# this is the main function that can be used to download the data
def create_data_files(starting_index, ids_per_data_subset):
    # retrieving a list of all coin IDs
    # the initial API call returns a list of dictionaries with detailed information
    coin_list = cg.get_coins_list()
    list_length = len(coin_list)
    path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"

    while True:
        # printing the progress 
        progress = int(starting_index / list_length * 100)
        print("The overall progress is: " + str(progress) + "%")
        
        ids, ending_index = filtered_ids(coin_list, starting_index, ids_per_data_subset)
        # exporting the data in a data frame to a path specified by the user
        # the starting index is stored
        retrieving_data(ids).to_csv(path + "/cg_data.csv" + str(starting_index), index=False)
        starting_index = ending_index

        if starting_index + 1 == list_length:
            break
    
        if starting_index != 0:
            print("The last successful starting index is: " + str(starting_index))

# calling the main function
create_data_files(0, 100)