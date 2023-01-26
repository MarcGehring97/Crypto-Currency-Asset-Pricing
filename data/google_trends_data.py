"""
The function "retrieve_data" has the following arguments:
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- kw_list: A list of all search keywords for which the time series should be downloaded or returned. The default is the keyword "Bitcoin".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "retrieve_data" returns a pd dataframe with columns for date and interest_over_time. The data is available at a monthly frequency only.
Interest over time represents search interest relative to the highest point on the chart for the given region and time.
"""

__all__ = ["retrieve_data"]

def retrieve_data(start_date, end_date, path="", kw_list=["Bitcoin"], download=True):

    import os, pandas as pd

    from pytrends.request import TrendReq

    pytrend = TrendReq()

    timeframe = str(start_date.date()) + " " + str(end_date.date())

    pytrend.build_payload(kw_list, timeframe=timeframe)

    # interest represents search interest relative to the highest point on the chart for the given region and time
    # a value of 100 is the peak popularity for the term
    interest = pytrend.interest_over_time()
    interest = interest.reset_index(level=0)
    interest = interest.drop("isPartial", axis=1)
    interest = interest.rename(columns={"Bitcoin": "interest_over_time"})

    data = pd.DataFrame.from_records(interest)
    data["date"] = pd.to_datetime(data["date"])
    data = data.drop_duplicates(subset="date")
    data.set_index("date", inplace=True, drop=True)
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    data = data.reindex(date_range)

    print("Count total NaN at each column in a dataframe:\n\n", data.isnull().sum())

    if path != "":
        file_names = os.listdir(path)

    if download:
        if "google_trends_data.csv" not in file_names:
            data.to_csv(path + "/google_trends_data.csv")
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/google_trends_data.csv")
                data.to_csv(path + "/google_trends_data.csv")
            else:
                print("Could not create a new file.")
    else: 
        return data

# import pandas as pd
# print(retrieve_data(start_date=pd.to_datetime("2014-01-01"), end_date=pd.to_datetime("today"), download=False).head(62))