"""

Using blockchain.com's (formally blockchain.info) REST API

charts can be found at
https://www.blockchain.com/explorer/charts/

the API documentation can be found at
https://www.blockchain.com/explorer/api/charts_api


the data series are available for different dates

the disprepancy is small and the values are replaced by n/a for the time being

"""
# import os
# os.system("python -m pip install requests")

import requests, datetime, pandas as pd, time

charts = ["n-unique-addresses", "n-transactions"]
# charts.append("n-payments")
# the API for the chart n-payments us currently not available
# as for Unix timestamps the timezone is UTC
start_dates =["2011-01-01T12:00:00", "2017-01-01T12:00:00"]
# start_dates.append("2023-01-01T12:00:00")


print(time.mktime((2011, 1, 1, 12, 0, 0, 4, 1, -1)))
print(time.mktime((2011, 1, 1, 12, 1, 1, 4, 1, -1)))

start_date = datetime.datetime.fromtimestamp(time.mktime((2011, 1, 1, 12, 0, 0, 4, 1, -1)))
end_date = datetime.datetime.fromtimestamp(time.time())

# creating a list of all days between the start says and today
dates_old = pd.date_range(start_date, end_date - datetime.timedelta(days=1), freq='d')
dates = []
for date in dates_old:
    dates.append(date.to_pydatetime().date())

historic_data = {"dates": dates, "unique_addresses": [], "transactions": []}
# historic_data[payments] = []

for chart in charts:
    for start_date in start_dates:
        # if one chooses a longer time period than 6 years, the API starts returning fewer data points
        api_url = "https://api.blockchain.info/charts/" + chart + "?timespan=6years&start=" + start_date + "&format=json"
        response = requests.get(api_url)
        if response.status_code != 200:
            print("There was an error")
        # turn the downloaded data into a dictionary
        data = response.json()
        # the x-values are Unix timestamps
        # checking if the starting and ending dates are correct
        # print(datetime.datetime.fromtimestamp(data["values"][0]["x"]))
        # print(datetime.datetime.fromtimestamp(data["values"][-1]["x"]))

        # selecting the appropriate list in the dictionary
        key = ""
        match chart:
            case "n-unique-addresses":
                key = "unique_addresses"
            case "n-transactions":
                key = "transactions"
            case "n-payments":
                key = "payments"

        for date1 in dates:
            # match the dates in the data to all dates in the time period
            match_found = False
            for i in range(len(data["values"])):
                date2 = datetime.date.fromtimestamp(data["values"][i]["x"])
                # if there is a match
                if date1 == date2:
                    index_found = True
                    break
            if match_found:
                historic_data[key].append(data["values"][i]["y"])
            else:
                historic_data[key].append(None)
        
print(len(historic_data["dates"]))
print(len(historic_data["unique_addresses"]))
print(len(historic_data["transactions"]))
print(historic_data["unique_addresses"])
print(historic_data["transactions"])
historic_data = pd.DataFrame.from_dict(historic_data)

path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"

historic_data.to_csv(path + "/blockchain.com_data.csv", index=False)