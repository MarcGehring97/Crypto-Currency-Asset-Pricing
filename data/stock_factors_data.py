"""
The code allows the user to download the daily data of the five Fama-French and the momentum factor from 
http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library/f-f_factors.html. Fama-French use a different source for the risk-free 
rate (Ibbotson and Associates). The data might not be available until the indicated end date. Weekends are not the data and the data is 
replaced by NaN.

The function "retrieve_data" has the following arguments:
- start_date: The start date specified in the data_processing file. 
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "retrieve_data" returns a pd dateframe with columns for date, Mkt-RF, SMB, HML, RMW, CMA, and Mom.
"""

__all__ =["retrieve_data"]

def retrieve_all(start_date, end_date, path="", download=True):

    import pandas as pd, os, pandas_datareader.data as web  # module for reading datasets directly from the web
    from pandas_datareader.famafrench import get_available_datasets

    if path != "":
        file_names = os.listdir(path)

    datasets = get_available_datasets()
    print("The number of datasets is: " + str(len(datasets)))
    factors = ["5", "Momentum"]

    output = {}
    for factor in factors:
        factor_data = [dataset for dataset in datasets if factor in dataset and "Factor" in dataset]
        # print(factor_data)
        # index 1 is daily data (0 for monthly)
        data_dict = web.DataReader(factor_data[1], "famafrench", start=start_date, end=end_date)
        # print(data_dict["DESCR"])
        daily_data = data_dict[0]
        output[factor] = daily_data
    
    data = output["5"].drop("RF", axis=1)
    data["Mom"] = output["Momentum"]["Mom   "].tolist()
    data.index.names = ["date"]
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    data = data.reindex(date_range)
    
    print("Count total NaN at each column in a dataframe:\n\n", data.isnull().sum()) 

    if download:
        if "stock_factors_data.csv" not in file_names:
            data.to_csv(path + "/stock_factors_data.csv")
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/stock_factors_data.csv")
                data.to_csv(path + "/stock_factors_data.csv")
            else:
                print("Could not create a new file.")
    else: 
        return data
    
# import pandas as pd
# print(retrieve_all(start_date=pd.to_datetime("2014-01-01"), end_date=pd.to_datetime("today"), download=False).head(50))

