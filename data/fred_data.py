"""
The IDs of the series can be found on the sites of the series themselves. They are also in the name of the path in the site URL. There are
missing values for all weekends. Those values can be easily replaced, for example, by the values closest to them.

The function "retrieve_data" has the following arguments:
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- series_ids: All the series IDs to retrieve or download the data for. The default are the two available metrics of the three that are needed.
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "retrieve_data" returns a pd dataframe with columns for date, DEXUSAL, DEXCAUS, DEXUSEU, DEXSIUS, DEXUSUK
"""

__all__ = ["retrieve_data"]

def retrieve_data(start_date, end_date, path="", series_ids=["DGS1MO", "DEXUSAL", "DEXCAUS", "DEXUSEU", "DEXSIUS", "DEXUSUK"], download=True):

    import requests, os, pandas as pd, datetime, time, numpy as np, json

    api_key = json.load(open("/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/fred_key.json"))["api_key"]

    start_date_dt = datetime.datetime.strptime(str(start_date), "%Y-%m-%d").date()
    end_date_dt = datetime.datetime.strptime(str(end_date), "%Y-%m-%d").date()
    start_date = datetime.datetime.fromtimestamp(time.mktime((start_date_dt.year, start_date_dt.month, start_date_dt.day, 12, 0, 0, 4, 1, -1)))
    end_date = datetime.datetime.fromtimestamp(time.mktime((end_date_dt.year, end_date_dt.month, end_date_dt.day, 12, 0, 0, 4, 1, -1)))

    # creating a list of all days between the start day and today
    days_old = pd.date_range(start_date, end_date, freq='d')
    days = []
    # the months list contains the ID of the month for every respective day in the month
    for date in days_old:
        days.append(date.to_pydatetime().date())

    historic_data = {"date": days}

    for series_id in series_ids:
        historic_data[series_id] = []

    if path != "":
        file_names = os.listdir(path)
    
    for series_id in series_ids:
        # for this series, we need more data and hence need to make 2 API calls
        if series_id == "DGS1MO":
            api_url1 = "https://api.stlouisfed.org/fred/series/observations?series_id=" + series_id + "&file_type=json&realtime_start=" + str(start_date_dt) + "&realtime_end=" + "2017-12-31" + "&api_key=" + api_key
            api_url2 = "https://api.stlouisfed.org/fred/series/observations?series_id=" + series_id + "&file_type=json&realtime_start=" + "2018-01-01" + "&realtime_end=" + str(end_date_dt) + "&api_key=" + api_key
            response1 = requests.get(api_url1)
            response2 = requests.get(api_url2)

            if response1.status_code != 200 or response2.status_code != 200:
                print("There was an error")
                continue

            data = response1.json()["observations"] + response2.json()["observations"]

        else:    
            api_url = "https://api.stlouisfed.org/fred/series/observations?series_id=" + series_id + "&file_type=json&realtime_start=" + str(start_date_dt) + "&realtime_end=" + str(end_date_dt) + "&api_key=" + api_key
            response = requests.get(api_url)

            if response.status_code != 200:
                print("There was an error")
                continue

            data = response.json()["observations"]

        for day in days:
            # match the days to all dates in the respective data set
            match_found = False
            for i in range(len(data)):
                # if there is a match
                if str(day) == str(data[i]["date"]):
                    match_found = True
            if match_found and not str(data[i]["value"]) == "null":
                historic_data[series_id].append(data[i]["value"])
            else:
                # otherwise a NaN is added when the date does not exist in the data or when the data is "null"
                historic_data[series_id].append(np.nan)

    historic_data = pd.DataFrame.from_dict(historic_data)

    print("Count total NaN at each column in a dataframe:\n\n", historic_data.isnull().sum())

    if download:
        if "fred_data.csv" not in file_names:
            historic_data.to_csv(path + "/fred_data.csv", index=False)
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/fred_data.csv")
                historic_data.to_csv(path + "/fred_data.csv", index=False)
            else:
                print("Could not create a new file.")
    else:
        return historic_data

# import datetime
# print(retrieve_data(start_date="2014-01-01", end_date=str(datetime.date.today()), series_ids = ["DGS1MO"], download=False).head())