"""
The function "retrieve_data" can be used to download or return a csv file of price, market cap, and volume timeseries data for a group of 
cryptocurrencies that can be filtered by specified criteria. The data is retrieved using the free CoinGecko (https://www.coingecko.com/) API. 
The corresponding Github repository can be found at https://github.com/man-c/pycoingecko/ and the documentation of the CoinGecko API can be 
found at https://www.coingecko.com/en/api/documentation. The user should make sure that the required libraries are installled (pandas and 
pycoingecko).

The user may also add further filtering criteria and change the chosen time period. For convenience, this code allows the user to download 
the data set in several data subsets. The user may specify the size of every data subset. The progress may sometimes stop due to the limit 
imposed upon the number of API calls per minute of 50. The last successfully used incremented ending index for the while loop in the function 
"create_data_files" is printed and also indicated at the end of each file name. If the process is interrupted for any reason the user can 
call the main function again with a new starting index "retrieve_data(starting_index=<old ending index>)".

The function "retrieve_data" has the following arguments:
- start_date: The start date specified in the data_processing file. 
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- starting_index: The user can use the variable according to the description in the previous paragraph. The default is the overall starting
                  index of 0.
- ids_per_data_subset: This is the size of each data subset if the user intends to download the data is data subsets. The default is 100.
- download: Whether the user wants to download the data or get them returned. The default is True.
- pro_key: If the user has a CoinGecko API Pro key, he/she can use it here. The API calls will no longer have the restrictions described 
           above. The default is "".

The function "retrieve_data" returns a pd dataframe with columns for id, dates, prices, market_caps, and total_volumes
- 
"""

__all__ = ["retrieve_data"]

def retrieve_data(start_date, end_date, path="", starting_index=0, ids_per_data_subset=100, download=True, pro_key=""):
    
    import time, pandas as pd, datetime, pycoingecko, os

    if pro_key == "":
        cg = pycoingecko.CoinGeckoAPI()
    else:
        cg = pycoingecko.CoinGeckoAPI(api_key=pro_key)

    # checking whether the API works
    print(cg.ping())

    # this function takes in a crude list of coin IDs and returns a filtered list of a predefined size
    def filter_ids(coin_list, starting_index, ids_per_data_subset):
        index = int(starting_index)
        percentage_counter = 1
        ids = []
        print("The filtering progress for this data (sub)set is: ")
        while True:
            # if the end of the whole list is reached
            if index < len(coin_list):
                # if the number of IDs per data subset is reached
                if len(ids) < ids_per_data_subset:
                    # filtering out coins with a current market capitalization of less than 1m USD
                    try:
                        if cg.get_coin_by_id(coin_list[index]["id"])["market_data"]["market_cap"]["usd"] >= 1000000:
                            # this takes a while
                            ids.append(coin_list[index]["id"])
                    except:
                        print("There is a missing key for " + coin_list[index]["id"]+ " but the process will continue")
                    index += 1
                    # printing the progress 
                    progress = int(len(ids) / ids_per_data_subset * 100)
                    if progress > percentage_counter:
                        percentage_counter += 1
                        print(str(progress) + "%")
                else:
                    break
            else:
                break

        # checking for duplicate IDs (there are usually none)
        for id in ids:
            if ids.count(id) > 1:
                print("This ID occurs more than once: " + id + ".")
        
        return (ids, index)

    # Creating Unix timestamps
    start_date_dt = datetime.datetime.strptime(str(start_date), "%Y-%m-%d").date()
    end_date_dt = datetime.datetime.strptime(str(end_date), "%Y-%m-%d").date()
    start_date = time.mktime((start_date_dt.year, start_date_dt.month, start_date_dt.day, 12, 0, 0, 4, 1, -1))
    end_date = time.mktime((end_date_dt.year, end_date_dt.month, end_date_dt.day, 12, 0, 0, 4, 1, -1))
    # info regarding the arguments can be found at https://docs.python.org/3/library/time.html#time.struct_time

    # creating a data frame with the historic data (dates, prices, market capitalizations, and total volumes) for each coin ID
    def retrieving_data(ids):
        print("The retrieval progress for this data sub(set) is: ")
        percentage_counter = 0
        list_length = len(ids)
        counter = 0
        # filling a dictionary with historic data (dates, prices, market capitalizations, and total volumes) for each coin ID
        historic_data = {"id": [], "dates": [], "prices": [], "market_caps": [], "total_volumes": []}
        for id in ids:
            # printing the progress
            counter += 1
            progress = int(counter / list_length * 100)
            if progress > percentage_counter:
                percentage_counter += 1
                print(str(progress) + "%")
            
            # retrieving the data 
            data = cg.get_coin_market_chart_range_by_id(id=id, vs_currency="usd", from_timestamp=start_date, to_timestamp=end_date)
            # this takes a while

            # filtering out any coins for which not all information is available
            if len(data["prices"]) != 0 and len(data["market_caps"]) != 0 and len(data["total_volumes"]) != 0 and len(data["prices"]) == len(data["market_caps"]) == len(data["total_volumes"]):
                for i in range(len(data["prices"])):
                    historic_data["id"].append(id)
                    historic_data["dates"].append(datetime.date.fromtimestamp(data["prices"][i][0] / 1000))
                    # converting the Unix datestamps to the POSIX format without hours, minutes, and seconds
                    # dividing by 1000 to convert from milliseconds to seconds
                    historic_data["prices"].append(data["prices"][i][1])
                    historic_data["market_caps"].append(data["market_caps"][i][1])
                    historic_data["total_volumes"].append(data["total_volumes"][i][1])
                
        historic_data = pd.DataFrame.from_dict(historic_data)
        # the data series are of different lengths depending on the availability of historic data
        return historic_data

    # this is the main function that can be used to download the data
    def create_data_files(path, starting_index, ids_per_data_subset, download):
        # retrieving a list of all coin IDs
        # the initial API call returns a list of dictionaries with detailed information
        
        coin_list = cg.get_coins_list()
        list_length = len(coin_list)
        if ids_per_data_subset == "All":
            ids_per_data_subset = list_length
            if download:
                print("Downloading all data.")
            else:
                print("Returning all data.")
        dfs = []

        if path != "":
            file_names = os.listdir(path)

        while True:
            # printing the progress 
            progress = int(starting_index / list_length * 100)
            print("The overall progress is: " + str(progress) + "%")
            
            ids, ending_index = filter_ids(coin_list, starting_index, ids_per_data_subset)
            # the retrieved index is already incremented since the incrementation happens after the ID is added to the IDs list
            # exporting the data in a dataframe to a path specified by the user
            # the ending index is stored
            # if the last ending index in the file name is 700, for example, and the process stopped, it can be continued by calling create_data_files() with a starting_index of 700
            # it can be the case, though, that if you download different data packages on different days, the list of coins has changed
            
            df = retrieving_data(ids)
            if download:
                if "coingecko_data" + str(ending_index) + ".csv" not in file_names:
                    df.to_csv(path + "/coingecko_data" + str(ending_index) + ".csv", index=False)
                else:
                    if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                        os.remove("/coingecko_data" + str(ending_index) + ".csv")
                        df.to_csv(path + "/coingecko_data" + str(ending_index) + ".csv", index=False)
                    else:
                        print("Could not create a new file.")
            else:
                dfs.append(df)
            starting_index = ending_index

            if starting_index >= list_length:
                break
            
            print("The last successful ending index is: " + str(ending_index) + ".")

        if not download:
            if (len(dfs)) > 1:
                return pd.concat(dfs)
            else:
                return dfs[0]
    
    # here the function is called
    return create_data_files(path, starting_index, ids_per_data_subset, download)
    
# calling the main function
# print(retrieve_data(ids_per_data_subset = "All", download = False).head())
# to download the entire data set at once (not safe) insert "All" for the argument "ids_per_data_subset"

import datetime
retrieve_data(start_date="2014-01-01", end_date=str(datetime.date.today()), path="/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data/coingecko", starting_index=11561, ids_per_data_subset=100)

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

    import time, pycoingecko

    cg = pycoingecko.CoinGeckoAPI()

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
            print("Error, but continue.")
        counter += 1
        total_time1 += end1 - start1
        total_time2 += end2 - start2

    print("The two API calls were tested for " + str(counter) + " coins.")
    print("The average time for the first API call was " + str(total_time1 / counter) + ".")
    print("The average time for the second API call was " + str(total_time2 / counter) + ".")

# speed_test()

