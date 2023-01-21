# computing the zero-investment long-short strategies based on the size-related characteristics of market capitalization, price, maximum day price, and age
import data.fred_data as fred_data, helpers, pandas as pd, numpy as np, datetime, math, os, random
from wand.image import Image

# specify the data range for the analysis
# in the paper, the authors start on 2014-01-01 due to data availability
start_date = "2014-01-01"
end_date = str(datetime.date.today())

# select the path to the directory where you want to store the data
data_path = r"/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"

# the data was retrieved on 2023-01-13
daily_trading_data = pd.read_csv(data_path+"/cg_data.csv")
daily_trading_data["date"] = pd.to_datetime(daily_trading_data["date"])
daily_trading_data.set_index("date", inplace=True)

# all unique coin IDs
coin_ids = ["bitcoin", "ethereum", "ripple"]

coins_weekly_returns = {}
for coin_id in coin_ids:
    df = pd.DataFrame({"year": [], "week": [], "price": [], "market_cap": [], "total_volume": [], "return": []})
    df["year"] = [2014] * 52 + [2015] * 52 + [2016] * 52 + [2017] * 52 + [2018] * 52
    df["week"] = [i + 1 for i in range(52)] * 5
    df["price"] = random.sample(range(10, 300), 260)
    df["market_cap"] = random.sample(range(10000, 1000000), 260)
    df["total_volume"] = random.sample(range(10000, 1000000), 260)
    returns = random.sample(range(10, 300), 260)
    returns = [x / 1000 for x in returns]
    df["return"] = returns
    coins_weekly_returns[coin_id] = df

# computing the risk-free rate
# a pd dataframe with columns for date and DGS1MO
risk_free_rate = df
returns = random.sample(range(10, 300), 260)
returns = [x / 1000 for x in returns]
risk_free_rate["DGS1MO"] = returns



# function for the zero-investment long-short strategy
# it returns a list for the returns of the top 20% and the bottom 20% of a given characteristic
def quintile_returns(size_characteristic):
    first_quintile_returns = []
    second_quintile_returns = []
    third_quintile_returns = []
    fourth_quintile_returns = []
    fifth_quintile_returns = []
    # initializing the list of included coins
    first_quintile_coins = []
    second_quintile_coins = []
    third_quintile_coins = []
    fourth_quintile_coins = []
    fifth_quintile_coins = []
    # taking an arbitrary ID to loop through all weeks
    for i in coins_weekly_returns[coin_ids[0]].index:
        year = coins_weekly_returns[coin_ids[0]]["year"][i]
        week = coins_weekly_returns[coin_ids[0]]["week"][i]
        if year == 2014 and week == 1:
            # in the first week we have no quintile data (and also no return data)
            first_quintile_returns.append(np.nan)
            second_quintile_returns.append(np.nan)
            third_quintile_returns.append(np.nan)
            fourth_quintile_returns.append(np.nan)
            fifth_quintile_returns.append(np.nan)
        else:
            # computing the returns for the quintiles
            for quintile in ["first", "second", "third", "fourth", "fifth"]:
                match quintile:
                    case "first":
                        coins = first_quintile_coins
                    case "second":
                        coins = second_quintile_coins
                    case "third":
                        coins = third_quintile_coins
                    case "fourth":
                        coins = fourth_quintile_coins
                    case "fifth":
                        coins = fifth_quintile_coins
                returns = []
                market_caps = []
                for coin_id in coins:
                    coin_weekly_returns = coins_weekly_returns[coin_id]
                    coin_weekly_data = coin_weekly_returns[(coin_weekly_returns["year"] == year) & (coin_weekly_returns["week"] == week)]
                    # ignoring all NaNs
                    # the most convenient way to check if no cell value are NaN is by applying .isna().sum().sum()
                    if coin_weekly_data.isna().sum().sum() == 0:
                        returns.append(coin_weekly_data["return"].tolist()[0])
                        market_caps.append(coin_weekly_data["market_cap"].tolist()[0])
                # if all returns are NaN (for example, in the first week of the time period considered)
                if len(returns) == 0:
                    # if no value was added
                    match quintile:
                        case "first":
                            first_quintile_returns.append(np.nan)
                        case "second":
                            second_quintile_returns.append(np.nan)
                        case "third":
                            third_quintile_returns.append(np.nan)
                        case "fourth":
                            fourth_quintile_returns.append(np.nan)
                        case "fifth":
                            fifth_quintile_returns.append(np.nan)
                else:
                    # for every week add the value-weighted market return (the sumproduct of the returns and the market caps divided by the sum of the market caps) and the included coin IDs
                    weighted_average = (sum(x * y for x, y in zip(returns, market_caps)) / sum(market_caps))
                    match quintile:
                        case "first":
                            first_quintile_returns.append(weighted_average)
                        case "second":
                            second_quintile_returns.append(weighted_average)
                        case "third":
                            third_quintile_returns.append(weighted_average)
                        case "fourth":
                            fourth_quintile_returns.append(weighted_average)
                        case "fifth":
                            fifth_quintile_returns.append(weighted_average)

        # computing quintiles for the next week 
        characteristic_data_list = []
        coin_ids_copy = coin_ids.copy()
        for coin_id in coin_ids:
            characteristic_data = coins_weekly_returns[coin_id][(coins_weekly_returns[coin_id]["year"] == year) & (coins_weekly_returns[coin_id]["week"] == week)][size_characteristic]
            if not pd.isna(characteristic_data.tolist()[0]):
                characteristic_data_list.append(characteristic_data.tolist()[0])
            else:
                # ignoring all coins with missing values
                coin_ids_copy.remove(coin_id)
        # finding the coin ids of the top and bottom 20% market cap coins
        print(coin_ids_copy)
        print(characteristic_data_list)
        characteristic_data = pd.DataFrame({"coin_id": coin_ids_copy, size_characteristic: characteristic_data_list})
        first_quintile = characteristic_data.quantile(q=0.2, interpolation="nearest").tolist()[0]
        second_quintile = characteristic_data.quantile(q=0.4, interpolation="nearest").tolist()[0]
        third_quintile = characteristic_data.quantile(q=0.6, interpolation="nearest").tolist()[0]
        fourth_quintile = characteristic_data.quantile(q=0.8, interpolation="nearest").tolist()[0]
        # and the according coins
        first_quintile_coins = characteristic_data["coin_id"][characteristic_data[size_characteristic] <= first_quintile].tolist()
        second_quintile_coins = characteristic_data["coin_id"][(characteristic_data[size_characteristic] > first_quintile) & (characteristic_data[size_characteristic] <= second_quintile)].tolist()
        third_quintile_coins = characteristic_data["coin_id"][(characteristic_data[size_characteristic] > second_quintile) & (characteristic_data[size_characteristic] <= third_quintile)].tolist()
        fourth_quintile_coins = characteristic_data["coin_id"][(characteristic_data[size_characteristic] > third_quintile) & (characteristic_data[size_characteristic] <= fourth_quintile)].tolist()
        fifth_quintile_coins = characteristic_data["coin_id"][characteristic_data[size_characteristic] > fourth_quintile].tolist()

    return {"first": first_quintile_returns, "second": second_quintile_returns, "third": third_quintile_returns, "fourth": fourth_quintile_returns, "fifth": fifth_quintile_returns}

# downloading the data since the returns computation process might also take a long time
# if the file for weekly returns data does not already exist
if not os.path.exists(data_path + "/long_short_data_size.csv"):
    print("Computing the excess long-short strategies for the different size characteristics.")

    # creating the data for the size characteristics in coins_weekly_returns
    for coin_id in coin_ids:
        coin_weekly_returns = coins_weekly_returns[coin_id]
        # adding log_market_cap
        coin_weekly_returns["log_market_cap"] = np.log(coin_weekly_returns["market_cap"])
        # adding log_price
        coin_weekly_returns["log_price"] = np.log(coin_weekly_returns["price"])
        # adding log_max_price
        coin_daily_trading_data = daily_trading_data[daily_trading_data["id"] == coin_id]
        # these columns have already been checked
        coin_daily_trading_data = coin_daily_trading_data.drop(columns=["id", "market_cap", "total_volume"])
        coin_max_data = coin_daily_trading_data.resample("W-SUN").max()
        coin_weekly_returns["log_max_price"] = np.log(coin_max_data["price"])
        # adding age as the number of days listed since the first trading day of that respective coin (considering that the tiem period befin on 2014-01-01)
        age = []
        for i in coin_weekly_returns.index:
            first_date_found = False
            # the number of days to the date of the first price determines the age
            if pd.isna(coin_weekly_returns["price"][i]) and first_date_found == False:
                first_date_found = True
                
                first_date_year = coin_weekly_returns["year"][i]
                first_date_week = coin_weekly_returns["year"][i]
            
            if not first_date_found:
                # making 0 days the default for all days ebfore the first trading day
                age.append(0)
            else:
                year = coin_weekly_returns["year"][i]
                week = coin_weekly_returns["year"][i]
                age.append(((year - first_date_year) * 52 + week - first_date_week) * 7)
        coin_weekly_returns["age"] = age
        # adding the new dataframes back to the dict
        coins_weekly_returns[coin_id] = coin_weekly_returns

    # "risk_free_rate" now is a pd dataframe with columns for year, month, and DGS1MO (the risk-free rate)
    long_short_data = risk_free_rate
    size_characteristics = ["log_market_cap", "log_price", "log_max_price", "age"]
    for size_characteristic in size_characteristics:
        quintile_returns_data = quintile_returns(size_characteristic)
        for quintile in quintile_returns_data.keys():
            # computing the excess returns
            long_short_data[size_characteristic + "_" + quintile + "_quintile_return"] = quintile_returns_data[quintile] - long_short_data["DGS1MO"] / 100
        # computing the excess returns for the long-short strategy
        long_short_data[size_characteristic + "_excess_ls"] = long_short_data[size_characteristic + "_fifth_quintile_return"] - long_short_data[size_characteristic + "_first_quintile_return"] - long_short_data["DGS1MO"] / 100

    # downloading the data
    long_short_data.to_csv(data_path + "/long_short_data_size.csv")
else:
    # next, we need to load the data and "unwrap" it again
    print("The data for the excess long-short strategies for the different size characteristics has already been computed.")
    long_short_data = pd.read_csv(data_path + "/long_short_data_size.csv")

print("Table 2 lists the return predictor definitions and can be found in the paper.")

# inverting the colors in the PDF in case the user is using dark mode
if input("Is your editor is dark mode? y/n ") in ["Y", "y"]:
    invert = True
else:
    invert = False

print(long_short_data.head())


"""
# use "\symbol{37}" in the latex file for the percentage sign
helpers.render_size_strategy_returns(long_short_data, invert=invert)

# creates a temporary PDF file named "cover.pdf"
# repeating the process overwrites the file

pdf_path = os.getcwd() + "/cover.pdf"
# printing the PDF
# this code has to be in the main file
img = Image(filename=pdf_path, resolution=100)
img
"""



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