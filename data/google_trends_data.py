"""
The function "retrieve_data" has the following arguments:
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- kw_list: A list of all search keywords for which the time series should be downloaded or returned. The default is the keyword "Bitcoin".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "retrieve_data" returns a pd dataframe with columns for date and search_count
"""

__all__ = ["retrieve_data"]

def retrieve_data(start_date, end_date, path="", kw_list=["Bitcoin"], download=True):

    import os, datetime, time, pandas as pd, numpy as np

    from pytrends.request import TrendReq

    pytrend = TrendReq()

    timeframe = start_date + " " + end_date

    pytrend.build_payload(kw_list, timeframe=timeframe)

    # interest represents search interest relative to the highest point on the chart for the given region and time
    # a value of 100 is the peak popularity for the term
    interest = pytrend.interest_over_time()
    interest = interest.reset_index(level=0)
    interest = interest.drop("isPartial", axis=1)
    interest = interest.rename(columns={"Bitcoin": "search_count"})

    # change sampling frequency to daily
    start_date_dt = datetime.datetime.strptime(str(start_date), "%Y-%m-%d").date()
    start_date = datetime.datetime.fromtimestamp(time.mktime((start_date_dt.year, start_date_dt.month, start_date_dt.day, 12, 0, 0, 4, 1, -1)))
    end_date = datetime.datetime.strptime(str(end_date), "%Y-%m-%d")

    # creating a list of all days between the start day and today
    days_old = pd.date_range(start_date, end_date, freq='d')
    days = []
    # the months list contains the ID of the month for every respective day in the month
    for date in days_old:
        days.append(date.to_pydatetime().date())

    data = {"date": days, "search_count": []}
    for day in days:
        # match the days to all dates in the respective data set
        match_found = False
        for i in range(len(interest)):
            # if there is a match
            # for every day that the month of the date matches to
            if str(day)[:7] == str(interest["date"][i])[:7]:
                match_found = True
                break
        if match_found:
            data["search_count"].append(interest["search_count"][i])
        else:
            # otherwise a NaN is added when the date does not exist in the data or when the data is "null"
            data["search_count"].append(np.nan)

    data = pd.DataFrame.from_dict(data)

    print("Count total NaN at each column in a dataframe:\n\n", data.isnull().sum())

    if path != "":
        file_names = os.listdir(path)

    if download:
        if "google_trends_data.csv" not in file_names:
            data.to_csv(path + "/google_trends_data.csv", index=False)
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/google_trends_data.csv")
                data.to_csv(path + "/google_trends_data.csv", index=False)
            else:
                print("Could not create a new file.")
    else: 
        return data

import datetime
# print(retrieve_data(start_date="2014-01-01", end_date=str(datetime.date.today()), download=False).head())