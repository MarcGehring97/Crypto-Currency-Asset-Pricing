import pandas as pd
data_path = r"/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"
daily_trading_data = pd.read_csv(data_path+"/cg_data.csv")

coin_daily_prices = daily_trading_data[daily_trading_data["id"] == "bitcoin"]
print(coin_daily_prices[[date[:4] == "2014" for date in list(coin_daily_prices["date"])]]["market_cap"].isna().sum())
print(len(coin_daily_prices[[date[:4] == "2014" for date in list(coin_daily_prices["date"])]]))

counter = 0
if coin_daily_prices[[date[:4] == "2014" for date in list(coin_daily_prices["date"])]]["market_cap"].isna().sum() == 0:
    counter += 1

print(counter)