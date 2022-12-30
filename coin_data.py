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
index "create_data_files(starting_index = <new starting index>)".
"""

# import os
# os.system("pip3 install pandas")
# os.system("python3 -m pip install -U pycoingecko")

import time, pandas as pd, import datetime
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
        if index < len(coin_list):
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
                progress = int(len(ids) / ids_per_data_subset * 100)
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
    percentage_counter = 1
    list_length = len(ids)
    for id in ids:
        # printing the progress 
        progress = int(len(historic_data["id"]) / list_length * 100)
        if progress >= percentage_counter:
            percentage_counter += 1
            print(str(progress) + "%")
        
        # retrieving the data 
        data = cg.get_coin_market_chart_range_by_id(id=id, vs_currency="usd", from_timestamp=start_date, to_timestamp=end_date)
        # this might take a while

        dates = []
        prices = []
        for i in data["prices"]:
            # converting the Unix datestamps to the POSIX format without hours, minutes, and seconds
            date = datetime.datetime.fromtimestamp(i[0])
            date.replace(hour=0, minute=0, second=0, microsecond=0)
            dates.append(date)
            prices.append(i[1])
        
        market_caps = []
        for i in data["market_caps"]:
            market_caps.append(i[1])
        
        total_volumes = []
        for i in data["total_volumes"]:
            total_volumes.append(i[1])

        # filtering out any coins for which not all information is available
        if len(dates) != 0 and len(prices) != 0 and len(market_caps) != 0 and len(total_volumes) != 0:
            historic_data["id"].append(id)
            historic_data["dates"].append(dates)
            historic_data["prices"].append(prices)
            historic_data["market_caps"].append(market_caps)
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
        # the retrieved index is already incremented since the incrementation happens after the ID is added to the IDs list
        # exporting the data in a data frame to a path specified by the user
        # the ending index is stored
        retrieving_data(ids).to_csv(path + "/coingecko_data" + str(ending_index) + ".csv", index=False)
        starting_index = ending_index

        if starting_index >= list_length:
            break
        
        # this information can be used in case something goes wrong so that one does not have to start over
        print("The last successful ending index is: " + str(ending_index))

# calling the main function
# to download the entire data set at once (not safe) insert "len(cg.get_coins_list())" for the second argument
create_data_files(0, 100)

"""
The code below can be used to test how long the API takes to process different kinds of calls. The result here is that
for a sample of 100 coins the average time for the first API call was 2.58 and for the second API call 9.06. Hence,
if one assumes that 90% of the calls are getting filtered out by the > 1m USD condition for the market cap, it still
makes sense to first run the filtering process and then the data retieval rather than downloading all data and then
using conditions to filter. This is how the average times for the two options compare: 2.58 + (1 - 0.1) * 9.06 = 3.49 
< 9.06. One has to keep in mind, though, that the API may have stopped at one calls more often than at the other.
"""

# speed test for the two different API calls
def speed_test(number_of_coins = 100):
    start_date = time.mktime((2011, 1, 1, 12, 0, 0, 4, 1, -1))
    end_date = time.time()
    coin_list = cg.get_coins_list()[:number_of_coins]
    total_time1 = 0
    total_time2 = 0
    counter = 0

    for id in coin_list:
        try: 
            id = id["id"]
            start1 = time.time()
            cg.get_coin_by_id(id)["market_data"]["market_cap"]["usd"]
            end1 = time.time()
            start2 = time.time()
            cg.get_coin_market_chart_range_by_id(id=id, vs_currency="usd", from_timestamp=start_date, to_timestamp=end_date)
            end2 = time.time()
        except:
            print("Error, but continue")
        counter += 1
        total_time1 += end1 - start1
        total_time2 += end2 - start2

    print("The two API calls were tested for " + str(counter) + " coins")
    print("The average time for the first API call was " + str(total_time1 / counter))
    print("The average time for the second API call was " + str(total_time2 / counter))

# speed_test()

