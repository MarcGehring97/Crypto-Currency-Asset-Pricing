"""
This file can be used to merge several downloaded data frames into one large data set. The user has to
indicate the correct path destination where the data subsets are stored. The output is then stored in
the same directory.

The function "merge" has the following arguments:
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "merge" returns a a list of tuples with the cryptocurrency ID and a pd dataframe with columns for id, date, price, market_cap, and total_volume
"""

# reading all data subsets as data frames
def merge(path="", download=True):

    import pandas as pd, os, time, datetime

    file_names = os.listdir(path)
    dfs = []
    for file_name in file_names:
        if file_name[-4:] == ".csv" and file_name[-11:] != "cg_data.csv":
            df = pd.read_csv(path + "/" + file_name)
            dfs.append(df)

    # combining all dataframes in the dfs list
    data = pd.concat(dfs)
            
    # removing any duplicate rows
    data = data.drop_duplicates()
    data = data.rename(columns={"dates": "date", "prices": "price", "market_caps": "market_cap", "total_volumes": "total_volume"})

    # creating single dataframes for every ID and also comparing the dates to all days in the time period
    ids = list(data["id"].unique())
    dfs = []
    for id in ids:
        df = data[data["id"] == id]
        dfs.append(df)

    start_date = datetime.datetime.fromtimestamp(time.mktime((2011, 1, 1, 12, 0, 0, 4, 1, -1)))
    end_date = datetime.datetime.fromtimestamp(time.time())

    # creating a list of all days between the start day and today
    days_old = pd.date_range(start_date, end_date, freq='d')
    days = []
    # the months list contains the ID of the month for every respective day in the month
    for date in days_old:
        days.append(date.to_pydatetime().date())

    # checking for every ID dataframe whether all days are in the time series => if a day is missing, insert a "NaN"
    output = []
    for df in dfs:
        output_dict = {"date": days, "price": [], "market_cap": [], "total_volume": []}
        # iterating over all three variables
        for var in ["price", "market_cap", "total_volume"]:
            for day in days:
                # match the days to all dates in the respective data set
                match_found = False
                for i in range(len(df)):
                    # if there is a match
                    # if the value of the variable is 0.0 it means that it is missing
                    if str(day) == df["date"][i] and df[var][i] != 0.0:
                        match_found = True
                if match_found:
                    output_dict[var].append(df[var][i])
                else:
                    # otherwise a "NaN" is added when the date does not exist in the data or when the data is "null"
                    output_dict[var].append("NaN")
        output_df = pd.DataFrame(output_dict)
        output.append((id, output_df))

    if download:
        if "cg_data.csv" not in file_names:
            output.to_csv(path + "/cg_data.csv", index=False)
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/cg_data.csv")
                output.to_csv(path + "/cg_data.csv", index=False)
            else:
                print("Could not create a new file.")
    else:
        return output

merge(path="/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data")