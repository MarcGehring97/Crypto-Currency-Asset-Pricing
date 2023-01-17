"""
This file can be used to merge several downloaded data frames into one large data set. The user has to
indicate the correct path destination where the data subsets are stored. The output is then stored in
the same directory. The merging and processing takes a long time.

The function "merge" has the following arguments:
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "merge" returns a pd dataframe with columns for id, date, price, market_cap, and total_volume
"""

__all__ = ["merge"]

# reading all data subsets as data frames
def merge(start_date, end_date, path="", download=True):

    import pandas as pd, os, datetime, numpy as np

    file_names = os.listdir(path + "/coingecko")

    dfs = []
    for file_name in file_names:
        if file_name[-4:] == ".csv" and file_name[-11:] != "cg_data.csv":
            df = pd.read_csv(path + "/coingecko" + "/" + file_name)
            dfs.append(df)

    # combining all dataframes in the dfs list
    data = pd.concat(dfs)
            
    # removing any duplicate rows
    # data = data.drop_duplicates()
    data = data.rename(columns={"dates": "date", "prices": "price", "market_caps": "market_cap", "total_volumes": "total_volume"})

    # creating single dataframes for every ID and also comparing the dates to all days in the time period
    ids = list(data["id"].unique())
    dfs = []
    for id in ids:
        df = data[data["id"] == id]
        dfs.append(df)
    
    start_date = datetime.datetime.strptime(str(start_date), "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(str(end_date), "%Y-%m-%d").date()

    # creating a list of all days between the start day and today
    days_old = pd.date_range(start_date, end_date, freq='d')
    days = []
    # the months list contains the ID of the month for every respective day in the month
    for date in days_old:
        days.append(date.to_pydatetime().date())

    # checking for every ID dataframe whether all days are in the time series => if a day is missing, insert a NaN
    output_dfs = []
    print("The progress is: ")
    percentage_counter = 0
    for df in dfs:
        # printing the progress
        progress = int(df.index[0] / len(data) * 100)
        if progress > percentage_counter:
            percentage_counter += 1
            print(str(progress) + "%")
        output_dict = {"date": days, "price": [], "market_cap": [], "total_volume": []}
        # finding the first date where all column values are non-missing to skip all previous rows
        min_date = str(datetime.date.today())
        # the index in dataframes does not necessarily start at 0, especially when the dataframe got sliced => df.index
        for i in df.index:
            # if the next value is not equal to 0.0, the minimum date gets set
            if df["price"][i] != 0.0 and df["price"][i] != "" and df["market_cap"][i] != 0.0 and df["market_cap"][i] != "" and df["total_volume"][i] != 0.0 and df["total_volume"][i] != "":
                min_date = str(df["date"][i])
                break
            # if all data is 0.0 the min_date will still be today and all values will be turned into NaN
        # match the days to all dates in the respective data set
        for day in days:
            # skipping all days where there is missing data
            if datetime.datetime.strptime(str(day), "%Y-%m-%d").date() < datetime.datetime.strptime(min_date, "%Y-%m-%d").date():
                output_dict["price"].append(np.nan)
                output_dict["market_cap"].append(np.nan)
                output_dict["total_volume"].append(np.nan)
                continue
            match_found = False
            for i in df.index:
                # if the value of the variable is 0.0 it means that it is missing
                # only adding rows if all three column values are non-missing
                if str(day) == str(df["date"][i]) and df["price"][i] != 0.0 and df["price"][i] != "" and df["market_cap"][i] != 0.0 and df["market_cap"][i] != "" and df["total_volume"][i] != 0.0 and df["total_volume"][i] != "":
                    match_found = True
            # if there is a match
            if match_found:
                output_dict["price"].append(df["price"][i])
                output_dict["market_cap"].append(df["market_cap"][i])
                output_dict["total_volume"].append(df["total_volume"][i])
            else:
                # otherwise a NaN is added when the date does not exist in the data or when the data is "null"
                output_dict["price"].append(np.nan)
                output_dict["market_cap"].append(np.nan)
                output_dict["total_volume"].append(np.nan)
        output_df = pd.DataFrame(output_dict)
        id = df["id"][df.index[0]]
        # adding the ID
        output_df.insert(0, "id", [id] * len(output_df))
        output_dfs.append(output_df)
        print("The current ID is " + id + ".")
    print("100%")

    output = pd.concat(output_dfs)
    output = output.reset_index(drop=True)
    
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

# import datetime
# merge(start_date="2014-01-01", end_date=str(datetime.date.today()),path=r"/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data")