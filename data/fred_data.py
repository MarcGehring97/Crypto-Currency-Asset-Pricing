"""
The IDs of the series can be found on the sites of the series themselves. They are also in the name of the path in the site URL. There are
missing values for all weekends. Those values can be easily replaced, for example, by the values closest to them.

The function "retrieve_data" has the following arguments:
- path: The path where the user intends to store the data. The default is "".
- series_ids: All the series IDs to retrieve or download the data for. The default are the two available metrics of the three that are needed.
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "retrieve_data" returns a pd dataframe with columns for date, DEXUSAL, DEXCAUS, DEXUSEU, DEXSIUS, DEXUSUK
"""

__all__ = ["retrieve_data"]

def retrieve_data(path="", series_ids=["DEXUSAL", "DEXCAUS", "DEXUSEU", "DEXSIUS", "DEXUSUK"], download=True):

    import requests, os, pandas as pd, datetime, time

    api_key = "f32180d669b6c6eccde4ddfba4c49a7c"

    start_date = datetime.datetime.fromtimestamp(time.mktime((2011, 1, 1, 12, 0, 0, 4, 1, -1)))
    end_date = datetime.datetime.fromtimestamp(time.time())

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
        api_url = "https://api.stlouisfed.org/fred/series/observations?series_id=" + series_id + "&file_type=json&realtime_start=2011-01-01&api_key=" + api_key
        print(api_url)

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
                if str(day) == data[i]["date"]:
                    match_found = True
            if match_found and not str(data[i]["value"]) == "null":
                historic_data[series_id].append(data[i]["value"])
            else:
                # otherwise a "NaN" is added when the date does not exist in the data or when the data is "null"
                historic_data[series_id].append("NaN")

    for series_id in series_ids:
        print("For the " + series_id + " data there are " + str(historic_data[series_id].count("NaN")) + " NaN values.")
    
    historic_data = pd.DataFrame.from_dict(historic_data)

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

# print(retrieve_data(download=False).head(100))