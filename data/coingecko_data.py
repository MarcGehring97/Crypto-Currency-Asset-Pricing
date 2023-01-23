"""
The function "retrieve_data" can be used to download or return a csv file of price, market cap, and volume timeseries data for a group of 
cryptocurrencies that can be filtered by specified criteria. The data is retrieved using the free CoinGecko (https://www.coingecko.com/) API. The 
corresponding Github repository can be found at https://github.com/man-c/pycoingecko/ and the documentation of the CoinGecko API can be found at 
https://www.coingecko.com/en/api/documentation. The user should make sure that the required libraries are installled (pandas and pycoingecko).

The user may also add further filtering criteria and change the chosen time period. For convenience, this code allows the user to download the data set 
in several data subsets. The  user may specify the size of every data subset. The progress may sometimes stop due to the limit imposed upon the number 
of API calls per minute of 50. The last successfully used incremented ending index for the while loop in the function "create_data_files" is printed 
and also indicated at the end of each file name. If the process is interrupted for any reason the user can call the main function again with a new 
starting index "retrieve_data(starting_index=<old ending index>)".

The function "retrieve_data" has the following arguments:
- start_date: The start date specified in the data_processing file. 
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- starting_index: The user can use the variable according to the description in the previous paragraph. The default is the overall starting index of 0.
- ids_per_data_subset: This is the size of each data subset if the user intends to download the data is data subsets. The default is 100.
- download: Whether the user wants to download the data or get them returned. The default is True.
- pro_key: If the user has a CoinGecko API Pro key, he/she can use it here. The API calls will no longer have the restrictions described above. The default is "".

The function "retrieve_data" returns a pd dataframe with columns for coin_id, prices, market_caps, and total_volumes. The index are the dates.
"""

__all__ = ["retrieve_data"]

from re import X


def retrieve_data(start_date, end_date, path="", starting_index=0, ids_per_data_subset=100, download=True, pro_key=""):
    
    import time, pandas as pd, datetime, pycoingecko, os, numpy as np

    if pro_key == "":
        cg = pycoingecko.CoinGeckoAPI()
    else:
        cg = pycoingecko.CoinGeckoAPI(api_key=pro_key)

    # checking whether the API is working
    print(cg.ping())

    # this function takes in a crude list of coin IDs and returns a filtered list of a predefined size
    def filter_ids(coin_list, starting_index, ids_per_data_subset):
        index = int(starting_index)
        coin_ids = []
        while True:
            # if the end of the whole list is reached
            if index < len(coin_list):
                # if the number of IDs per data subset is reached
                if len(coin_ids) < ids_per_data_subset:
                    try:
                        # filtering out coins with a current market capitalization of less than 1m USD
                        # if cg.get_coin_by_id(coin_list[index]["id"])["market_data"]["market_cap"]["usd"] >= 1000000:
                        # this takes a while
                        coin_ids.append(coin_list[index]["id"])
                    except:
                        print("There is a missing key for " + coin_list[index]["id"]+ " but the process will continue")
                    index += 1
                    # printing the progress 
                else:
                    break
            else:
                break

        # checking for duplicate IDs (there are usually none)
        for coin_id in coin_ids:
            if coin_ids.count(coin_id) > 1:
                print("This ID occurs more than once: " + coin_id + ".")
        
        return (coin_ids, index)

    # Creating Unix timestamps
    start_date_unix = time.mktime((start_date.year, start_date.month, start_date.day, 12, 0, 0, 4, 1, -1))
    end_date_unix = time.mktime((end_date.year, end_date.month, end_date.day, 12, 0, 0, 4, 1, -1))
    # info regarding the arguments can be found at https://docs.python.org/3/library/time.html#time.struct_time

    # creating a data frame with the historic data (dates, prices, market capitalizations, and total volumes) for each coin ID
    def retrieving_cg_data(coin_ids):
        print("The retrieval progress for this data (sub)set is: ")
        percentage_counter = 0
        list_length = len(coin_ids)
        counter = 0
        # filling a dictionary with historic data (dates, prices, market capitalizations, and total volumes) for each coin ID
        dfs = []
        # getting the complete date range
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        for coin_id in coin_ids:
            coin_df = pd.DataFrame({"date": date_range})
            coin_df.set_index("date", inplace=True, drop=True)
            # printing the progress
            counter += 1
            progress = int(counter / list_length * 100)
            if progress > percentage_counter:
                percentage_counter += 1
                print(str(progress) + "%")
            
            try:
                # retrieving the data 
                data = cg.get_coin_market_chart_range_by_id(id=coin_id, vs_currency="usd", from_timestamp=start_date_unix, to_timestamp=end_date_unix)
                # this takes a while
                # filtering out coins that have no data for any of the variables
                vars = ["prices", "market_caps", "total_volumes"]
                if not any(len(data[var]) == 0 for var in vars):
                    # looping through all varialbes
                    for var in vars:
                        df = pd.DataFrame.from_records(data[var])
                        df = df.rename(columns={0: "date", 1: var})
                        df["date"] = pd.to_datetime(df["date"], unit="ms", origin="unix")
                        df = df.drop_duplicates(subset="date")
                        df.set_index("date", inplace=True, drop=True)
                        # adding NaN values for the missing data
                        df = df.reindex(date_range)
                        # adding NaN values for 0.0 values
                        df[var] = df[var].replace(0.0, np.nan)
                        coin_df[var] = df[var].tolist()
                
                coin_df.insert(0, "coin_id", coin_id, True)
                dfs.append(coin_df)
            except Exception:
                print(Exception)
                print("The data could not be downloaded but the process continues.")

        df = pd.concat(dfs)
        df = df.rename(columns={"prices": "price", "market_caps": "market_cap", "total_volumes": "total_volume"})
        # the data series are of different lengths depending on the availability of historic data
        return df

    # this is the main function that can be used to download the data
    def create_data_files(path, starting_index, ids_per_data_subset, download):
        # retrieving a list of all coin IDs
        # the initial API call returns a list of dictionaries with detailed information
        
        coin_list = cg.get_coins_list()

        """
        new = []
        for dict in coin_list:
            if dict["id"] == "bitcoin" or dict["id"] == "ethereum":
                new.append(dict)
        coin_list = new
        """

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
            
            coin_ids, ending_index = filter_ids(coin_list, starting_index, ids_per_data_subset)
            # the retrieved index is already incremented since the incrementation happens after the ID is added to the IDs list
            # exporting the data in a dataframe to a path specified by the user
            # the ending index is stored
            # if the last ending index in the file name is 700, for example, and the process stopped, it can be continued by calling create_data_files() with a starting_index of 700
            # it can be the case, though, that if you download different data packages on different days, the list of coins has changed                

            df = retrieving_cg_data(coin_ids)
            if download:
                coingecko_path = path + "/coingecko"
                # creating the folder /coingecko_data in the data folder
                if not os.path.exists(coingecko_path):
                    os.makedirs(coingecko_path)
                if "coingecko_data" + str(ending_index) + ".csv" not in file_names:
                    df.to_csv(coingecko_path + "/coingecko_data" + str(ending_index) + ".csv")
                else:
                    if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                        os.remove("/coingecko_data" + str(ending_index) + ".csv")
                        df.to_csv(coingecko_path + "/coingecko_data" + str(ending_index) + ".csv")
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

"""
# calling the main function
# print(retrieve_data(ids_per_data_subset = "All", download = False).head())
# to download the entire data set at once (not safe) insert "All" for the argument "ids_per_data_subset"
import pandas as pd
start_date = pd.to_datetime("2014-01-01")
end_date = pd.to_datetime("today")

while True:
    try:
        print(retrieve_data(start_date=start_date, end_date=end_date, path="", starting_index=0, ids_per_data_subset=100, download=False).head())
        break
    except:
        continue
"""

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

    for coin in coin_list:
        try: 
            coin_id = coin["id"]
            start1 = time.time()
            cg.get_coin_by_id(coin_id)["market_data"]["market_cap"]["usd"]
            end1 = time.time()
            start2 = time.time()
            cg.get_coin_market_chart_range_by_id(id=coin_id, vs_currency="usd", from_timestamp=start_date, to_timestamp=end_date)
            end2 = time.time()
        except:
            print("Error, but continue.")
        counter += 1
        total_time1 += end1 - start1
        total_time2 += end2 - start2

    print("The two API calls were tested for " + str(counter) + " coins.")
    print("The average time for the first API call was " + str(total_time1 / counter) + ".")
    print("The average time for the second API call was " + str(total_time2 / counter) + ".")