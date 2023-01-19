"""
This file can be used to convert daily to weekly data. The idea is the following: The program runs through all years in the provided data set
separately. In every year, it defines a week after seven days have passed; there are some extra things to consider for the last week. In every
week, the program runs through all columns in reverse temporal order and adds the first non-NaN value to the output 
row that is in turn added to the output dataframe. For this function, it is important that the input data does not include any missing days.

The function "retrieve_data" has the following arguments:
- data: The data, for which to turn daily into weekly data points.
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- name: The name of the data set. The default is "".
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "weekly_data" returns a pd dateframe with columns for year/week (replacing the date column) and the other given columns
"""

def weekly_data(data, start_date, end_date, mode="last_day",name="", path="", download=True):
    
    import datetime, pandas as pd, os, numpy as np

    if path != "":
        file_names = os.listdir(path)
    # first divide data into weeks
    start_year = start_date[:4]
    end_year = end_date[:4]
    years = []
    year = start_year
    while int(year) + 1 <= int(end_year):
        years.append(year)
        year = str(int(year) + 1)
  
    # keeping the structure of the previous data file
    weekly_data = data[0:0]
    weekly_data.insert(0, "year", [])
    weekly_data.insert(1, "week", [])
    weekly_data = weekly_data.drop("date", axis=1)
  
    for year in years:
        date = datetime.datetime.strptime(year + "-01-01", "%Y-%m-%d").date()
        days = 0
        week = 0
        while days <= 364:
        # increasing the date by one day
            new_date = date + datetime.timedelta(days=days) 
            
            # after every week
            if (days + 1) % 7 == 0:
                week += 1

                # decrementing the date to see if there are any other useful data points in this week
                days_dec = 0

                limit = 6
                # the last week of the year, which is longer by 1 or 2 days
                if week == 52:
                    # if the year is a leap year
                    if int(year) % 4 == 0:
                        limit += 2
                    else:
                        limit += 1
                
                # creating an empty row
                new_data_row = data[0:0]
                new_data_row.insert(0, "year", [])
                new_data_row.insert(1, "week", [])
                new_data_row = new_data_row.drop("date", axis=1)

                columns = new_data_row.columns.tolist()
                columns.remove("year")
                columns.remove("week")

                if mode == "last_day":
                    for column in columns:
                        match_found = False
                        while days_dec <= limit:
                            # the decremented date
                            date_dec = new_date - datetime.timedelta(days=days_dec)

                            data_row = data[data["date"] == str(date_dec)]
                            # adding the first other day that has no NaN values
                            if data_row[column].isna().sum() == 0:
                                new_data_row[column] = data_row[column]
                                match_found = True
                                break
                            
                            days_dec += 1
                        
                        # if no match is found, add NaN
                        if not match_found:
                            new_data_row[column] = np.nan
                
                elif mode == "max_day":
                    for column in columns:
                        match_found = False
                        max_val = data[data["date"] == str(new_date)][column]
                        while days_dec <= limit:
                            # the decremented date
                            date_dec = new_date - datetime.timedelta(days=days_dec)

                            data_row = data[data["date"] == str(date_dec)]
                            # adding the first other day that has no NaN values
                            if data_row[column].isna().sum() == 0 and data_row[column].tolist()[0] >= max_val.tolist()[0]:
                                max_val = data_row[column]
                                match_found = True
                            
                            days_dec += 1

                        if match_found:
                            new_data_row[column] = max_val
                        
                        # if no match is found, add NaN
                        else:
                            new_data_row[column] = np.nan
                
                # inserting the week
                new_data_row["year"] = year
                new_data_row["week"] = str(week)
                            
                # adding the row to the output dataframe
                weekly_data = pd.concat([weekly_data, new_data_row])
            
            days += 1

    weekly_data = weekly_data.reset_index(drop=True)

    if download:
        if name + "_weekly_data.csv" not in file_names:
            weekly_data.to_csv(path + "/" + name + "_weekly_data.csv", index=False)
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/" + name + "_weekly_data.csv")
                weekly_data.to_csv(path + "/" + name + "_weekly_data.csv", index=False)
            else:
                print("Could not create a new file.")
    else: 
        return weekly_data

"""
import datetime, pandas as pd
start_date = "2014-01-01"
end_date = "2023-01-13"
path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data/coingecko/coingecko"
data = pd.read_csv(path + "/cg_data.csv")
print(data.head())
coin_ids = pd.unique(data["id"])
coin_data = data[data["id"] == "ethereum"]
weekly_data(data=coin_data, start_date= start_date, end_date=end_date, name="test1", path=path)
"""
