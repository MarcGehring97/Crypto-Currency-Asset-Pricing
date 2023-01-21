"""
Using blockchain.com's (formerly blockchain.info) REST API one can retrieve the data that proxy for the network effect of user adoption. 
Unfortunately, there is no longer a chart showing the number of wallet users. The chart for the number of payments is available, but the API 
does not seem to work. The according code pieces were commented out. One can find additional charts at 
https://www.blockchain.com/explorer/charts/ and add them to the data retrieval process by adding their names to the "charts" argument. The 
API documentation can be found at https://www.blockchain.com/explorer/api/charts_api. The data series may differ in terms of their dates; for
some dates data might be missing. Therefore, all data series are compared to a complete data time series and the gaps are filled with NaN. 
This disprepancy is small, though.

The function "retrieve_data" has the following arguments:
- start_date: The start date specified in the data_processing file. 
- end_date: The start date specified in the data_processing file.
- charts: A list of all charts for which the time series should be downloaded or returned.
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "retrieve_data" returns a pd dateframe with columns for date, n-unique-addresses, and n-transactions
"""

__all__ = ["retrieve_data"]

def retrieve_data(start_date, end_date, charts, path="", download=True):

    import requests, datetime, pandas as pd, time, os, numpy as np
    from dateutil.relativedelta import relativedelta

    start_date_dt = datetime.datetime.strptime(str(start_date), "%Y-%m-%d").date()
    end_date_dt = datetime.datetime.strptime(str(end_date), "%Y-%m-%d").date()
    start_dates = [start_date]
    start_dates.append(str(start_date_dt + relativedelta(years=6)))
    end_dates = []
    end_dates.append(str(start_date_dt + relativedelta(years=6) - relativedelta(days=1)))
    end_dates.append(str(start_date_dt + relativedelta(years=12) - relativedelta(days=1)))
    start_date = datetime.datetime.fromtimestamp(time.mktime((start_date_dt.year, start_date_dt.month, start_date_dt.day, 12, 0, 0, 4, 1, -1)))
    end_date = datetime.datetime.fromtimestamp(time.mktime((end_date_dt.year, end_date_dt.month, end_date_dt.day, 12, 0, 0, 4, 1, -1)))

    # creating a list of all days between the start day and today
    days_old = pd.date_range(start_date, end_date, freq='d')
    days = []
    for date in days_old:
        days.append(date.to_pydatetime().date())

    historic_data = {"date": days}
    # add an empty list for every chart to store the time series data in
    for chart in charts:
        historic_data[chart] = []

    if path != "":
        file_names = os.listdir(path)

    for chart in charts:
        for i in range(len(start_dates)):
            # if one chooses a longer time period than 6 years, the API starts returning fewer data points
            api_url = "https://api.blockchain.info/charts/" + chart + "?timespan=6years&start=" + start_dates[i] + "&format=json"
            response = requests.get(api_url)
            if response.status_code != 200:
                print("There was an error")
            # turn the downloaded data into a dictionary
            data = response.json()
            # the x-values are Unix timestamps
            # checking if the starting and ending dates are correct
            # print(datetime.datetime.fromtimestamp(data["values"][0]["x"]))
            # print(datetime.datetime.fromtimestamp(data["values"][-1]["x"]))

            # the code below creates a date subset according to start date
            start_date = start_dates[i]
            end_date = end_dates[i]
            days_subset = []
            # to track when the start date is
            begin = False
            for day in days:
                if str(day) == start_date:
                    begin = True
                if str(day) == end_date:
                    days_subset.append(day)
                    begin = False
                if begin:
                    days_subset.append(day)

            for day in days_subset:
                # match the dates in the dates subset to all dates in the respective data set
                match_found = False
                for i in range(len(data["values"])):
                    date = datetime.date.fromtimestamp(data["values"][i]["x"])
                    # if there is a match
                    if str(day) == str(date):
                        match_found = True
                        break
                if match_found:
                    historic_data[chart].append(data["values"][i]["y"])
                else:
                    # otherwise a NaN is added when the date does not exist in the data
                    historic_data[chart].append(np.nan)
    historic_data = pd.DataFrame.from_dict(historic_data)
    
    print("Count total NaN at each column in a dataframe:\n\n", historic_data.isnull().sum())

    if download:
        if "blockchain_com_data.csv" not in file_names:
            historic_data.to_csv(path + "/blockchain_com_data.csv")
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/blockchain_com_data.csv")
                historic_data.to_csv(path + "/blockchain_com_data.csv")
            else:
                print("Could not create a new file.")
    else: 
        return historic_data

import datetime
# print(retrieve_data(start_date="2014-01-01", end_date=str(datetime.date.today()), charts=["n-unique-addresses", "n-transactions"], download=False).head())

# print(retrieve_data(charts=["n-unique-addresses", "n-transactions", "n-payments"], download=False).head())
# throws an error