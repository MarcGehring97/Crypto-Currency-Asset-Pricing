import time, pandas as pd, datetime, pycoingecko, os

pro_key = ""

if pro_key == "":
    cg = pycoingecko.CoinGeckoAPI()
else:
    cg = pycoingecko.CoinGeckoAPI(api_key=pro_key)

coin_list = cg.get_coins_list()

print(coin_list)

start_date = "2014-01-01"
end_date = str(datetime.date.today())

start_date_dt = datetime.datetime.strptime(str(start_date), "%Y-%m-%d").date()
end_date_dt = datetime.datetime.strptime(str(end_date), "%Y-%m-%d").date()
start_date = time.mktime((start_date_dt.year, start_date_dt.month, start_date_dt.day, 12, 0, 0, 4, 1, -1))
end_date = time.mktime((end_date_dt.year, end_date_dt.month, end_date_dt.day, 12, 0, 0, 4, 1, -1))

data = cg.get_coin_market_chart_range_by_id(id="bitcoin", vs_currency="usd", from_timestamp=start_date, to_timestamp=end_date)

"""
prices = data["prices"]
market_caps = data["market_caps"]
total_volumes = data["total_volumes"]

print(len(prices))
print(len(market_caps))
print(len(total_volumes))

for i in range(len(prices)):
    print(prices[i])
    print(market_caps[i])
    print(total_volumes[i])
    if i > 500:
        break
"""