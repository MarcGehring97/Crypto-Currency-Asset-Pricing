"""
This file can be used to convert daily to weekly data. The idea is the following: The program runs through all years in the provided data set
separately. In every year, it defines a week after seven days have passed; there are some extra things to consider for the last week. In every
week, the program runs through all columns in reverse temporal order and adds the first non-NaN value to the output 
row that is in turn added to the output dataframe. For this function, it is important that the input data does not include any missing days.

The function "retrieve_data" has the following arguments:
- path: The path where the user intends to store the data. The default is "".
- data: The data, for which to turn daily into weekly data points.
- name: The name of the data set. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "weekly_data" returns a pd dateframe with columns for year/week (replacing the date column) and the other given columns
"""

def weekly_data(data, name="", path="", download=True):
    
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
  
    weekly_data = data[0:0]
    # keeping the structure of the previous data file
    weekly_data.insert(0, "year/week", [])
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
                new_data_row.insert(0, "year/week", [])
                new_data_row = new_data_row.drop("date", axis=1)

                columns = new_data_row.columns.tolist()
                columns.remove("year/week")

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
                
                # inserting the week
                new_data_row["year/week"] = year + "/" + str(week)
                            
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
end_date = str(datetime.date.today())
path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"
data = pd.read_csv(path + "/stock_factors_data.csv")
print(data.head())

print(weekly_data(data=data, name="stock_factors", download=False).head(55))
"""