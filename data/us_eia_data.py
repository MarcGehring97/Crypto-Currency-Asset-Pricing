"""
The documentation of the US Energy Information Administration API can be found at https://www.eia.gov/opendata/documentation.php. One can
easily generate the URLs for the GET requests by selecting the appropriate table at https://www.eia.gov/opendata/browser/. A major part of
the preprocessing is done here. For the daily data sets (net_generation and demand), the data series begins in 2018 and one has to retrieve
for the data for every day individually because the data sets are very large.

The function "retrieve_data" has the following arguments:
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "retrieve_data" returns a pd dataframe with columns for date, average_price, net_generation, demand
"""

__all__ = ["retrieve_data"]

def retrieve_data(start_date, end_date, path="", download=True):

    import requests, pandas as pd, os, numpy as np

    api_urls = {}
    api_urls["average_price"] = "https://api.eia.gov/v2/electricity/retail-sales/data/?api_key=xxxx&frequency=monthly&data[0]=price&start=2011-01&sort[0][column]=customers&sort[0][direction]=desc&offset=0&facets[stateid][]=US&length=1000000000000000&facets[sectorid][]=ALL"
    api_urls["net_generation"] = "https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/?api_key=xxxx&frequency=daily&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=1000000000000000"
    api_urls["demand"] = "https://api.eia.gov/v2/electricity/rto/daily-region-sub-ba-data/data/?api_key=xxxx&frequency=daily&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=1000000000000000"
    # the data can already be filtered through the URL by the parameter "facets"

    api_key = "jLRWwzxhWL7O85sOU5zE6l3FoRtB4FHbOMi1OqQW"

    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    historic_data = {"date": date_range}

    for api_url in api_urls:
        historic_data[api_url] = []

    if path != "":
        file_names = os.listdir(path)

    for var in api_urls:
        # to distinguish between monthly and daily data
        # this is necessary since one needs to retrieve the data for the other two variables for each date individually, starting from first day
        if var != "average_price":
            # this might take a while since there is a lot of data for every date
            for day in date_range:
                # skipping days before 2018 since there is no data available before then
                if int(str(day)[:4]) < 2018:
                    historic_data[var].append(np.nan)
                    # just append an NaN
                    continue
                api_url = api_urls[var].replace("xxxx", api_key) + "&start=" + str(day.date()) + "&end=" + str(day.date())
                print(api_url)
                try:
                    response = requests.get(api_url)
                    if response.status_code != 200:
                        print("There was an error for "+ api_url + ".")
                        print(response.json()["error"])
                        raise Exception
                except:
                    continue
                # turn the downloaded data into a dictionary
                data = response.json()["response"]["data"]
                # match the days to all dates in the respective data set
                match_found = False
                sum = 0
                # for net_generation computing the sum over all balancing authorities and all energy sources
                # for demand computing the sum over all industries and reporting agencies
                # both data series start in 2018 though
                # this can be seen on https://www.eia.gov/opendata/browser/electricity/rto/daily-fuel-type-data when selecting the starting date
                # for every day, one needs to find all data points
                for data_point in data:
                    # if there is a match
                    if data_point["value"] != "none":
                        sum += data_point["value"]
                        match_found = True
                        # at least one match was found
                if match_found:
                    historic_data[var].append(sum)
                else:
                    # otherwise a NaN is added when the date does not exist in the data or when the data is "null"
                    historic_data[var].append(np.nan)
        else:
            api_url = api_urls[var].replace("xxxx", api_key)
            print(api_url)
            try:
                response = requests.get(api_url)
                if response.status_code != 200:
                    print("There was an error for "+ api_url + ".")
                    print(response.json()["error"])
                    raise Exception
            except:
                continue
            # turn the downloaded data into a dictionary
            data = response.json()["response"]["data"]
            for day in date_range:
                # match the days to all dates in the respective data set
                match_found = False
                for i in range(len(data)):
                    # if there is a match
                    # for every day that the month of the date matches to
                    if str(day.date())[:7] == str(data[i]["period"]):
                        match_found = True
                        break
                if match_found and not str(data[i]["price"]) == "null":
                    historic_data[var].append(data[i]["price"])
                else:
                    # otherwise a NaN is added when the date does not exist in the data or when the data is "null"
                    historic_data[var].append(np.nan)

    historic_data = pd.DataFrame.from_dict(historic_data)
    historic_data = historic_data.drop_duplicates(subset="date")
    historic_data.set_index("date", inplace=True, drop=True)
    historic_data = historic_data.reindex(date_range)

    print("Count total NaN at each column in a dataframe:\n\n", historic_data.isnull().sum())
    
    if download:
        if "us_eia_data.csv" not in file_names:
            historic_data.to_csv(path + "/us_eia_data.csv", index=False)
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/us_eia_data.csv")
                historic_data.to_csv(path + "/us_eia_data.csv", index=False)
            else:
                print("Could not create a new file.")
    else:
        return historic_data

import pandas as pd
print(retrieve_data(start_date=pd.to_datetime("2014-01-01"), end_date=pd.to_datetime("today"), download=False).head(50))