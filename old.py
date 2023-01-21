"""
The "weekly_data" function can be used to convert daily to weekly data. The idea is the following: The program runs through all years in the 
provided data set separately. In every year, it defines a week after seven days have passed; there are some extra things to consider for the 
last week. In every week, the program runs through all columns in reverse temporal order and adds the first non-NaN value to the output row 
that is in turn added  to the output dataframe. For this function, it is important that the input data does not include any missing days.

The function "weekly_data" has the following arguments:
- data: The data, for which to turn daily into weekly data points.
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- mode: The selection method for the weekly data. The default is that the last available day is taken.
- name: The name of the data set. The default is "".
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "weekly_data" returns a pd dateframe with columns for year/week (replacing the date column) and the other given columns.
"""

def convert_to_weekly_frequency(data, start_date, end_date, mode="last_day",name="", path="", download=True):
    
    import datetime, pandas as pd, os, numpy as np

    if path != "":
        file_names = os.listdir(path)
    # first divide data into weeks
    start_year = start_date[:4]
    end_year = end_date[:4]
    years = []
    year = start_year
    while int(year) + 1 <= int(end_year):
        years.append(year)
        year = str(int(year) + 1)
  
    # keeping the structure of the previous data file
    weekly_data = data[0:0]
    weekly_data.insert(0, "year", [])
    weekly_data.insert(1, "week", [])
    weekly_data = weekly_data.drop("date", axis=1)
  
    for year in years:
        date = datetime.datetime.strptime(year + "-01-01", "%Y-%m-%d").date()
        days = 0
        week = 0
        while days <= 364:
        # increasing the date by one day
            new_date = date + datetime.timedelta(days=days) 
            
            # after every week
            if (days + 1) % 7 == 0:
                week += 1

                # decrementing the date to see if there are any other useful data points in this week
                days_dec = 0

                limit = 6
                # the last week of the year, which is longer by 1 or 2 days
                if week == 52:
                    # if the year is a leap year
                    if int(year) % 4 == 0:
                        limit += 2
                    else:
                        limit += 1
                
                # creating an empty row
                new_data_row = data[0:0]
                new_data_row.insert(0, "year", [])
                new_data_row.insert(1, "week", [])
                new_data_row = new_data_row.drop("date", axis=1)

                columns = new_data_row.columns.tolist()
                columns.remove("year")
                columns.remove("week")

                if mode == "last_day":
                    for column in columns:
                        match_found = False
                        while days_dec <= limit:
                            # the decremented date
                            date_dec = new_date - datetime.timedelta(days=days_dec)

                            data_row = data[data["date"] == str(date_dec)]
                            # adding the first other day that has no NaN values
                            if data_row[column].isna().sum() == 0:
                                new_data_row[column] = data_row[column]
                                match_found = True
                                break
                            
                            days_dec += 1
                        
                        # if no match is found, add NaN
                        if not match_found:
                            new_data_row[column] = np.nan
                
                elif mode == "max_day":
                    for column in columns:
                        match_found = False
                        max_val = data[data["date"] == str(new_date)][column]
                        while days_dec <= limit:
                            # the decremented date
                            date_dec = new_date - datetime.timedelta(days=days_dec)

                            data_row = data[data["date"] == str(date_dec)]
                            # adding the first other day that has no NaN values
                            if data_row[column].isna().sum() == 0 and data_row[column].tolist()[0] >= max_val.tolist()[0]:
                                max_val = data_row[column]
                                match_found = True
                            
                            days_dec += 1

                        if match_found:
                            new_data_row[column] = max_val
                        
                        # if no match is found, add NaN
                        else:
                            new_data_row[column] = np.nan
                
                # inserting the week
                new_data_row["year"] = year
                new_data_row["week"] = str(week)
                            
                # adding the row to the output dataframe
                weekly_data = pd.concat([weekly_data, new_data_row])
            
            days += 1

    weekly_data = weekly_data.reset_index(drop=True)

    if download:
        if name + "_weekly_data.csv" not in file_names:
            weekly_data.to_csv(path + "/" + name + "_weekly_data.csv")
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/" + name + "_weekly_data.csv")
                weekly_data.to_csv(path + "/" + name + "_weekly_data.csv")
            else:
                print("Could not create a new file.")
    else: 
        return weekly_data

"""
import datetime, pandas as pd
start_date = "2014-01-01"
end_date = "2023-01-13"
path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data/coingecko/coingecko"
data = pd.read_csv(path + "/cg_data.csv")
print(data.head())
coin_ids = pd.unique(data["id"])
coin_data = data[data["id"] == "ethereum"]
weekly_data(data=coin_data, start_date= start_date, end_date=end_date, name="test1", path=path)
"""





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

import data.coingecko_data as cg, data.blockchain_com_data as bc, data.coin_metrics_data as cm, data.us_eia_data as ue, data.google_trends_data as gt, data.twitter_data as tw, data.fred_data as fr, convert_frequency as cf
import datetime, pandas as pd

# parameters
start_date = "2014-01-01"
end_date = str(datetime.date.today())
path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"
path_coingecko = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data/coingecko"
charts = ["n-unique-addresses", "n-transactions"]
bearer_token = ""
query = ["Bitcoin"]
# charts.append("n-payments")
# the API for the chart n-payments is currently not available
metrics = ["AdrActCnt", "TxCnt"]
# keyword list
kw_list = ["Bitcoin"]
series_ids = ["DGS1MO", "DEXUSAL", "DEXCAUS", "DEXUSEU", "DEXSIUS", "DEXUSUK"]
# the last four letters describe the two currencies

"""
cg_data = ""
# the CoinGecko data needs to be uploaded from a file
# the data contains the variables id, date, price, market_cap, and total_volume
ue_data = ""
# the US Energy Information Administration data needs to be uploaded from a file
# the average price data is available only at monthly frequency => I hence used the respective value for all days in a given month
# the data contains the variables date, average_price, net_generation, demand
bc_data = bc.retrieve_data(start_date=start_date, end_date=end_date, charts=charts, download=False)
# the function "retrieve_data" returns a pd dateframe with columns for date, n-unique-addresses, and n-transactions
cm_data = cm.retrieve_data(mstart_date=start_date, end_date=end_date, etrics=metrics, download=False)
# the function "retrieve_data" returns a pd dataframe with columns for date, AdrActCnt and TxCnt
gt_data = gt.retrieve_data(start_date=start_date, end_date=end_date, kw_list=kw_list, download=False)
# the function "retrieve_data" returns a pd dataframe with columns for date and search_count
# the data is available only at monthly frquency => I hence used the respective value for all days in a given month
tw_data = tw.retrieve_data(start_date=start_date, end_date=end_date, query=query, bearer_token=bearer_token, download=False)
# the function "retrieve_data" returns a pd dataframe with columns for date and tweet_count
# the data point for today is not available => this series has hence one data point fewer
fr_data = fr.retrieve_data(start_date=start_date, end_date=end_date, series_ids=series_ids)
# the function "retrieve_data" returns a pd dataframe with columns for date, DGS1MO, DEXUSAL, DEXCAUS, DEXUSEU, DEXSIUS, DEXUSUK
# preprocessing the US EIA data
us_eia_data = ue.retrieve_data()
# look at the URL and the tables in the browser => what about missing values and the frequency?
"""



# cryptocurrency market return



# the base data are pd dataframes where the date is in the first row and all other variables in the following columns

# construct weekly coin returns

# A. Size Characteristics

# data of the risk-free rate
rf_daily = fr.retrieve_data(start_date=start_date, end_date=end_date, series_ids=["DGS1MO"])
rf_weekly = cf.weekly_data(rf_daily, download=False)

def sort(data):










# looking at rows where all values are not equal to 0
# interest = interest[(interest != 0).all(1)]

# drop all rows that have null values in all columns
# interest.dropna(how='all',axis=0, inplace=True)

# converting the data to weekly data
# formatting the dates to the same format
# df = df.rename(columns={"Bitcoin": "seach_count"})


# cryptocurrency_market_return = 






data = pd.read_csv(path + "/stock_factors_data.csv")
print(data.head())

weekly_data = cf.weekly_data(data, download=False)
print(weekly_data.head())
  
start_date = "2014-01-01"
end_date = str(datetime.date.today())
