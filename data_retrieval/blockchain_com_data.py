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
The function "retrieve_data" returns a pd dateframe with columns for date, n-unique-addresses, and n-transactions.
"""

__all__ = ["retrieve_data"]

def retrieve_data(start_date, end_date, charts, path="", download=True):
    import requests, pandas as pd, os
    from dateutil.relativedelta import relativedelta
    start_dates = [start_date, start_date + relativedelta(years=6)]
    end_dates = [start_date + relativedelta(years=6) - relativedelta(days=1), end_date]
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    historic_data = {"date": date_range}
    # add an empty list for every chart to store the time series data in
    for chart in charts:
        historic_data[chart] = []
    if path != "":
        file_names = os.listdir(path)
    for chart in charts:
        for i in range(len(start_dates)):
            # if one chooses a longer time period than 6 years, the API starts returning fewer data points
            api_url = "https://api.blockchain.info/charts/" + chart + "?timespan=6years&start=" + str(start_dates[i].date()) + "&format=json"
            response = requests.get(api_url)
            if response.status_code != 200:
                print("There was an error")
            # turn the downloaded data into a dictionary
            data = response.json()            
            data = pd.DataFrame.from_records(data["values"])
            # the x-values are Unix timestamps
            # the code below creates a date subset according to start date
            date_range = pd.date_range(start=start_dates[i], end=end_dates[i], freq="D")
            data = data.rename(columns={"x": "date", "y": chart})
            data["date"] = pd.to_datetime(data["date"], unit="s", origin="unix")
            data = data.drop_duplicates(subset="date")
            data.set_index("date", inplace=True, drop=True)
            data = data.reindex(date_range)
            historic_data[chart] += data[chart].tolist()
    historic_data = pd.DataFrame.from_dict(historic_data)
    historic_data.set_index("date", inplace=True, drop=True)
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