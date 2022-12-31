"""
Using blockchain.com's (formerly blockchain.info) REST API one can retrieve the data that proxy for the network effect of
user adoption. Unfortunately, there is no longer a chart showing the number of wallet users. The chart for the number of
payments is available, but the API does not seem to work. The according code pieces were commented out. One can find 
additional charts at https://www.blockchain.com/explorer/charts/ and add them to the data retrieval process by adding
their name to the "charts" list, the "historic_data" dictionary and the "chart" match case statement. The user should also
adapt the path where the data are downloaded to at the bottom of the file. The API documentation can be found at
https://www.blockchain.com/explorer/api/charts_api. The data series may differ in terms of their dates. Therefore, all data
series are compared to a complete data time series and the gaps are filled with "NaN". This disprepancy is small, though.
"""

# import os
# os.system("python -m pip install requests")

import requests, datetime, pandas as pd, time

charts = ["n-unique-addresses", "n-transactions"]
# charts.append("n-payments")
# the API for the chart n-payments us currently not available
# as for Unix timestamps the timezone is UTC
start_dates = ["2011-01-01", "2017-01-01"]
# start_dates.append("2023-01-01")
end_dates = ["2016-12-31", "2022-12-31"]
# end_dates.append("2028-12-31")

start_date = datetime.datetime.fromtimestamp(time.mktime((2011, 1, 1, 12, 0, 0, 4, 1, -1)))
end_date = datetime.datetime.fromtimestamp(time.time())

# creating a list of all days between the start day and today
dates_old = pd.date_range(start_date, end_date + datetime.timedelta(days=1), freq='d')
dates = []
for date in dates_old:
    dates.append(date.to_pydatetime().date())

# the key names here differ slightly from the chart names
historic_data = {"dates": dates, "unique_addresses": [], "transactions": []}
# historic_data[payments] = []

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

        # selecting the appropriate list in the dictionary
        key = ""
        match chart:
            case "n-unique-addresses":
                key = "unique_addresses"
            case "n-transactions":
                key = "transactions"
            case "n-payments":
                key = "payments"

        # the code below creates a date subset according to start date
        start_date = start_dates[i]
        end_date = end_dates[i]
        dates_subset = []
        # to track when the start date is
        begin = False
        for date in dates:
            if str(date) == start_date:
                begin = True
            if str(date) == end_date:
                dates_subset.append(date)
                begin = False
            if begin:
                dates_subset.append(date)

        for date1 in dates_subset:
            # match the dates in the dates subset to all dates in the respective data set
            match_found = False
            for i in range(len(data["values"])):
                date2 = datetime.date.fromtimestamp(data["values"][i]["x"])
                # if there is a match
                if date1 == date2:
                    match_found = True
                    break
            if match_found:
                historic_data[key].append(data["values"][i]["y"])
            else:
                # otherwise a "NaN" is added when the date does not exist in the data
                historic_data[key].append("NaN")

print("For the unique address data there are " + str(historic_data["unique_addresses"].count("NaN")) + " NaN values")
print("For the transaction data there are " + str(historic_data["transactions"].count("NaN")) + " NaN values")

historic_data = pd.DataFrame.from_dict(historic_data)

# insert your path here
path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"
historic_data.to_csv(path + "/blockchain.com_data.csv", index=False)
