import pandas as pd
data_path = r"/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"
# the data was retrieved on 2023-01-13
daily_trading_data = pd.read_csv(data_path+"/cg_data.csv")

df = daily_trading_data[daily_trading_data["id"] == "ripple"]


print(df[[i[:4] == "2014" for i in list(df["date"])]])