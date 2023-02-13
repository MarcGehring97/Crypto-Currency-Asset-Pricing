"""
The IDs of the series can be found on the sites of the series themselves. They are also in the name of the path in the site URL. There are
missing values for all weekends. Those values can be easily replaced, for example, by the values closest to them.

The function "retrieve_data" has the following arguments:
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- series_ids: All the series IDs to retrieve or download the data for. The default are the two available metrics of the three that are needed.
- download: Whether the user wants to download the data or get them returned. The default is True.
The function "retrieve_data" returns a pd dataframe with columns for date, DEXUSAL, DEXCAUS, DEXUSEU, DEXSIUS, DEXUSUK.
"""

__all__ = ["retrieve_data"]

def retrieve_data(start_date, end_date, path="", series_ids=["DGS1MO", "DEXUSAL", "DEXCAUS", "DEXUSEU", "DEXSIUS", "DEXUSUK"], download=True):
    import requests, os, pandas as pd, numpy as np, json, datetime
    api_key = json.load(open("/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/fred_key.json"))["api_key"]
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    historic_data = {"date": date_range}
    # the API call checks if the date is not in the future and is based on UTC-6
    if datetime.datetime.now().hour <= 6:
        end_date = end_date - pd.Timedelta(days=1)
    for series_id in series_ids:
        historic_data[series_id] = []
    if path != "":
        file_names = os.listdir(path)
    for series_id in series_ids:
        # for this series, we need more data and hence need to make 2 separate API calls
        if series_id == "DGS1MO":
            api_url1 = "https://api.stlouisfed.org/fred/series/observations?series_id=" + series_id + "&file_type=json&realtime_start=" + str(start_date.date()) + "&realtime_end=" + "2017-12-31" + "&api_key=" + api_key
            response1 = requests.get(api_url1)
            if end_date >= pd.to_datetime("2018-01-01"):
                api_url2 = "https://api.stlouisfed.org/fred/series/observations?series_id=" + series_id + "&file_type=json&realtime_start=" + "2018-01-01" + "&realtime_end=" + str(end_date.date()) + "&api_key=" + api_key
                response2 = requests.get(api_url2)
                if response1.status_code != 200 or response2.status_code != 200:
                    print("There was an error")
                    continue
                data = response1.json()["observations"] + response2.json()["observations"]
            else:
                if response1.status_code != 200:
                    print("There was an error")
                    continue
                data = response1.json()["observations"]
        else:    
            api_url = "https://api.stlouisfed.org/fred/series/observations?series_id=" + series_id + "&file_type=json&realtime_start=" + str(start_date.date()) + "&realtime_end=" + str(end_date.date()) + "&api_key=" + api_key
            response = requests.get(api_url)
            if response.status_code != 200:
                print("There was an error")
                continue
            data = response.json()["observations"]
        data = pd.DataFrame.from_records(data)
        data = data.rename(columns={"value": series_id})
        data["date"] = pd.to_datetime(data["date"])
        data = data.drop_duplicates(subset="date")
        data.set_index("date", inplace=True, drop=True)
        data = data[(data.index >= start_date) & (data.index <= end_date)]
        data = data.reindex(date_range)
        data[series_id] = data[series_id].replace(".", np.nan)
        data[series_id] = data[series_id].astype("float")
        historic_data[series_id] = data[series_id]
    historic_data = pd.DataFrame(historic_data)
    historic_data.set_index("date", inplace=True, drop=True)
    historic_data = historic_data.reindex(date_range)
    print("Count total NaN at each column in a dataframe:\n\n", historic_data.isnull().sum())
    if download:
        if "fred_data.csv" not in file_names:
            historic_data.to_csv(path + "/fred_data.csv")
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/fred_data.csv")
                historic_data.to_csv(path + "/fred_data.csv")
            else:
                print("Could not create a new file.")
    else:
        return historic_data