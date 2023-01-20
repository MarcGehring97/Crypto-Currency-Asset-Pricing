


















"""
# mock data
import pandas as pd, datetime, random
start_date = "2014-01-01"
end_date = "2023-01-12"

daily_trading_data = pd.DataFrame({"id": [], "date": [], "price": [], "market_cap": [], "total_volume": []})
daily_trading_data["id"] = ["Check"] * 90 + ["Out"] * 90
daily_trading_data["date"] = ["2014"] * 10 + ["2015"] * 10 + ["2016"] * 10 + ["2017"] * 10 + ["2018"] * 10 + ["2019"] * 10 + ["2020"] * 10 + ["2021"] * 10 + ["2022"] * 10 + ["2014"] * 10 + ["2015"] * 10 + ["2016"] * 10 + ["2017"] * 10 + ["2018"] * 10 + ["2019"] * 10 + ["2020"] * 10 + ["2021"] * 10 + ["2022"] * 10
daily_trading_data["price"] = random.sample(range(10, 300), 180)
daily_trading_data["market_cap"] = random.sample(range(10, 300), 180)
daily_trading_data["total_volume"] = random.sample(range(10, 300), 180)

market_weekly_returns = pd.DataFrame({"year": [], "week": [], "average_return": [], "included_ids": []})
market_weekly_returns["year"] = [2014] * 10 + [2015] * 10 + [2016] * 10 + [2017] * 10 + [2018] * 10 + [2019] * 10 + [2020] * 10 + [2021] * 10 + [2022] * 10
market_weekly_returns["week"] = random.sample(range(10, 300), 90)
returns = random.sample(range(10, 300), 90)
returns = [x / 1000 for x in returns]
market_weekly_returns["average_return"] = returns
market_weekly_returns["included_ids"] = [["Test"]] * 90

coins_weekly_returns = {"ethereum": None, "bitcoin": None, "ripple": None, "other": None}
for coin in coins_weekly_returns.keys():
    year = [2014] * 10 + [2015] * 10 + [2016] * 10 + [2017] * 10 + [2018] * 10 + [2019] * 10 + [2020] * 10 + [2021] * 10 + [2022] * 10
    week = random.sample(range(10, 300), 90)
    price = random.sample(range(10, 300), 90)
    market_cap = random.sample(range(10000, 300000), 90)
    volume = random.sample(range(10, 300), 90)
    returns = random.sample(range(10, 300), 90)
    returns = [x / 1000 for x in returns]

    df = pd.DataFrame({"year": year, "week": week, "price": price, "market_cap": market_cap, "volume": volume, "return": returns})
    coins_weekly_returns[coin] = df
"""