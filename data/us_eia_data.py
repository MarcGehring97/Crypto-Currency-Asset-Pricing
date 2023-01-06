"""
The documentation of the US Energy Information Administration API can be found at https://www.eia.gov/opendata/documentation.php. One can
easily generate the URLs for the GET requests by selecting the appropriate table at https://www.eia.gov/opendata/browser/. A major part of
the proprcessing is done here. For the daily data sets (net_generation and demand), the data series begins in 2018 and one has to retrieve
for the data for every day individually because the data sets are very large.

The function "retrieve_data" has the following arguments:
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "retrieve_data" returns a pd dataframe with columns for date, average_price, net_generation, demand
"""

__all__ = ["retrieve_data"]

def retrieve_data(path="", download=True):

    import requests, datetime, pandas as pd, time, os

    api_urls = {}
    api_urls["average_price"] = "https://api.eia.gov/v2/electricity/retail-sales/data/?api_key=xxxx&frequency=monthly&data[0]=price&start=2011-01&sort[0][column]=customers&sort[0][direction]=desc&offset=0&facets[stateid][]=US&length=1000000000000000&facets[sectorid][]=ALL"
    api_urls["net_generation"] = "https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/?api_key=xxxx&frequency=daily&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=1000000000000000"
    api_urls["demand"] = "https://api.eia.gov/v2/electricity/rto/daily-region-sub-ba-data/data/?api_key=xxxx&frequency=daily&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=1000000000000000"
    # the data can already be filtered through the URL by the parameter "facets"

    api_key = "jLRWwzxhWL7O85sOU5zE6l3FoRtB4FHbOMi1OqQW"

    start_date = datetime.datetime.fromtimestamp(time.mktime((2011, 1, 1, 12, 0, 0, 4, 1, -1)))
    end_date = datetime.datetime.fromtimestamp(time.time())

    # creating a list of all days between the start day and today
    days_old = pd.date_range(start_date, end_date, freq='d')
    days = []
    # the months list contains the ID of the month for every respective day in the month
    for date in days_old:
        days.append(date.to_pydatetime().date())

    historic_data = {"date": days}

    for api_url in api_urls:
        historic_data[api_url] = []

    if path != "":
        file_names = os.listdir(path)

    for api_url in api_urls:
        # to distinguish between monthly and daily data
        # this is necessary since one needs to retrieve the data for the other two variables for each date individually, starting from first day
        if api_url != "average_price":
            # this might take a while since there is a lot of data for every date
            for day in days:
                # skipping days before 2018 since there is no data available before then
                if int(str(day)[:4]) < 2018:
                    historic_data[api_url].append("NaN")
                    # just append an "NaN"
                    continue
                api_url = api_urls[api_url].replace("xxxx", api_key) + "&start=" + str(day) + "&end=" + str(day)
                print(api_url)
                response = requests.get(api_url)
                if response.status_code != 200:
                    print("There was an error for "+ api_url + ".")
                    print(response.json()["error"])
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
                    historic_data[api_url].append(sum)
                else:
                    # otherwise a "NaN" is added when the date does not exist in the data or when the data is "null"
                    historic_data[api_url].append("NaN")
        else:
            api_url = api_urls[api_url].replace("xxxx", api_key)
            print(api_url)
            response = requests.get(api_url)
            if response.status_code != 200:
                print("There was an error for "+ api_url + ".")
                print(response.json()["error"])
            # turn the downloaded data into a dictionary
            data = response.json()["response"]["data"]
            for day in days:
                # match the days to all dates in the respective data set
                match_found = False
                for i in range(len(data)):
                    # if there is a match
                    if str(day)[:7] == data[i]["period"]:
                        match_found = True
                        break
                if match_found and not str(data[i]["price"]) == "null":
                    historic_data[api_url].append(data[i]["price"])
                else:
                    # otherwise a "NaN" is added when the date does not exist in the data or when the data is "null"
                    historic_data[api_url].append("NaN")
                        
    for api_url in api_urls:
        print("For the " + api_url + " data there are " + str(historic_data[api_url].count("NaN")) + " NaN values.")

    historic_data = pd.DataFrame.from_dict(historic_data)
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

retrieve_data(path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data")