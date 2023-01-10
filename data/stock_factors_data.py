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

The function "retrieve_data" returns a pd dateframe with columns for date, Mkt-RF, SMB, HML, RMW, CMA, and Mom
"""

__all__ =["retrieve_data"]

def retrieve_all(start_date, end_date, path="", download=True):

    import pandas_datareader.data as web  # module for reading datasets directly from the web
    from pandas_datareader.famafrench import get_available_datasets

    import pandas as pd, time, os, numpy as np
    from dateutil.relativedelta import relativedelta

    if path != "":
        file_names = os.listdir(path)

    datasets = get_available_datasets()
    print("The number of datasets is: " + str(len(datasets)))
    factors=["5", "Momentum"]

    start_date_dt = datetime.datetime.strptime(str(start_date), "%Y-%m-%d").date()
    end_date_dt = datetime.datetime.strptime(str(end_date), "%Y-%m-%d").date()
    start_dates = [start_date]
    start_dates.append(str(start_date_dt + relativedelta(years=6)))
    end_dates = []
    end_dates.append(str(start_date_dt + relativedelta(years=6) - relativedelta(days=1)))
    end_dates.append(str(start_date_dt + relativedelta(years=12) - relativedelta(days=1)))
    start_date = datetime.datetime.fromtimestamp(time.mktime((start_date_dt.year, start_date_dt.month, start_date_dt.day, 12, 0, 0, 4, 1, -1)))
    end_date = datetime.datetime.fromtimestamp(time.mktime((end_date_dt.year, end_date_dt.month, end_date_dt.day, 12, 0, 0, 4, 1, -1)))

    # creating a list of all days between the start day and today
    days_old = pd.date_range(start_date, end_date, freq='d')
    days = []
    for date in days_old:
        days.append(date.to_pydatetime().date())

    output = {}
    for factor in factors:
        factor_data = [dataset for dataset in datasets if factor in dataset and "Factor" in dataset]
        # print(factor_data)
        # index 1 is daily data (0 for monthly)
        data_dict = web.DataReader(factor_data[1], "famafrench", start=start_date, end=end_date)
        # print(data_dict["DESCR"])
        daily_data = data_dict[0]
        daily_data.insert(0, "date", daily_data.index)
        daily_data = daily_data.reset_index(drop=True)
        # print(daily_data.head())
        output[factor] = daily_data
    
    all = output["5"].drop("RF", axis=1)
    all["Mom"] = output["Momentum"]["Mom   "]
    all['date'] = pd.to_datetime(all['date']).dt.date

    output = all[0:0]
    # keeping a dateframe with the old column names
    for day in days:
        # match the dates in the dates subset to all dates in the respective data set
        match_found = False
        for i in range(len(all)):
            date = all["date"][i]
            # if there is a match
            if str(day) == str(date):
                match_found = True
                break
        if match_found:
            output = pd.concat([output, all[all.index == i]])
        else:
            # otherwise a NaN is added when the date does not exist in the data
            output = pd.concat([output, pd.DataFrame({"date": day, "Mkt-RF": np.nan, "SMB": np.nan, "HML": np.nan, "RMW": np.nan, "CMA": np.nan, "Mom": np.nan}, index=[0])])
    
    output = output.reset_index(drop=True)

    for column in output.columns:
        if column != "date":
            print("For the " + column + " data there are " + str(output[column].isnull().sum()) + " NaN values.")    

    if download:
        if "stock_factors_data.csv" not in file_names:
            output.to_csv(path + "/stock_factors_data.csv", index=False)
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/stock_factors_data.csv")
                output.to_csv(path + "/stock_factors_data.csv", index=False)
            else:
                print("Could not create a new file.")
    else: 
        return output
    
import datetime
# retrieve_all(path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data", start_date="2014-01-01", end_date=str(datetime.date.today()))

