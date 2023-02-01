"""
import pandas as pd, numpy as np

num = 10


date_range = pd.date_range(start='2021-01-01', end='2021-12-31', freq='D').repeat(num)

data = {'date': date_range, 
        'price': np.random.normal(5, 50, num * 365),
        'return': np.random.normal(0, 0.5, num * 365),
        'market_return': np.random.normal(0.5, 0.5, num * 365),
        'market_cap': np.random.randint(1000000, 5000000000, num * 365),
        'total_volume': np.random.randint(10000, 500000, num * 365),                          
        'size_characteristic': np.random.normal(100, 50, num * 365)}
        
coin_id = []
for j in range(365):
    for i in range(num):                            
        coin_id += [i]
    
data["coin_id"] = coin_id
                            
df = pd.DataFrame(data)

# Replace random values in price, market_cap, and size_characteristic columns with missing values
cols = ['price', 'market_cap', 'size_characteristic', "market_return", "return"]
for col in cols:
    mask = np.random.rand(len(df)) < 0.3
    df[col][mask] = np.nan

df = df.sort_values(by=["coin_id", "date"])

df.set_index("date", inplace=True, drop=True)

def q_cut(x, size_characteristic):
    unique_var = len(x.unique())
    if x.isna().any() or x.isnull().any():
        unique_var -= 1
    
    match unique_var:
        case _ if unique_var >= 5:
            labels = [size_characteristic + "_q1", size_characteristic + "_q2", size_characteristic + "_q3", size_characteristic + "_q4", size_characteristic + "_q5"]
        # in the unlikely cases that there are fewer than 5 unique values for a given date
        case _ if unique_var == 4:
            labels = [size_characteristic + "_q1", size_characteristic + "_q2", size_characteristic + "_q4", size_characteristic + "_q5"]
        case _ if unique_var == 3:
            labels = [size_characteristic + "_q1", size_characteristic + "_q3", size_characteristic + "_q5"]
        case _ if unique_var == 2:
            labels = [size_characteristic + "_q1",size_characteristic + "_q5"]
        case _ if unique_var <= 1:
            labels = [size_characteristic + "_q3"]
    
    if unique_var == 0:
        return pd.Series(np.nan * len(x))

    output = pd.qcut(x, min(unique_var, 5), labels= labels, duplicates = "drop")
    return output

df["quintile"] = df.groupby(["date"])["return"].transform(lambda x: q_cut(x, "market_cap"))

# shifting the quintile information one period into the future
df["quintile"] = df.groupby("coin_id")["quintile"].shift(1)

# grouping the dataframe by quintile and date
grouped_df = df.groupby(["quintile", "date"])

# computing the market_cap-weighted return for each quintile in the following week
return_df = grouped_df.apply(lambda x: (x["return"] * x["market_cap"]).sum() / x["market_cap"].sum())

# resetting the index and renaming the columns
return_df = return_df.reset_index().rename(columns={0:"return"})

# return_df["date"] = return_df["date"].shift(1)

# pivoting the dataframe to get the return series for each quintile
return_df = return_df.pivot(index="date", columns="quintile", values="return")

# adding back NaN values
date_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq="W")
return_df = return_df.reindex(date_range)

print(return_df.head(50))
"""


import pandas as pd, os, subprocess, easydict, time, numpy as np
import data.fred_data, helpers

def convert_frequency(data, method, returns=False):
    
    date_range = pd.date_range(start=min(data.index), end=max(data.index), freq="W-SUN")
    
    if method == "last":
        data = data.groupby("coin_id").apply(lambda x: x.resample("W-SUN").last().reindex(date_range)).reset_index(level="coin_id", drop=True)
    elif method == "max":
        data = data.groupby("coin_id").apply(lambda x: x.resample("W-SUN").max().reindex(date_range)).reset_index(level="coin_id", drop=True)
    elif method == "mean":
        data = data.groupby("coin_id").apply(lambda x: x.resample("W-SUN").mean().reindex(date_range)).reset_index(level="coin_id", drop=True)
    
    # .pct_change() defaults to 0 for missing data
    if returns:
        data["return"] = data.groupby("coin_id")["price"].pct_change()

    data.index.name = "date"
    
    return data

# function for the zero-investment long-short strategy
# it returns a list for the returns of the top 20% and the bottom 20% of a given characteristic
def quintile_returns(df, size_characteristic):

    # adapting the Pandas qcut function for the edge cases
    def q_cut(x, size_characteristic):

        if x.isna().all():
            return pd.Series(np.nan * len(x))

        # this step is necessary since qcut sometimes assigns fewer categorites to the data than specified
        # Compute quantiles for all non-missing values
        mask = x.notna()        
        # computing quintiles manually since the Pandas's qcut() results in too many complications
        quintile_indices = np.digitize(x[mask], np.percentile(x[mask], [20, 40, 60, 80]), right=False)
        # mapping the quintiles to the appropriate quintile values 
        if len(np.unique(quintile_indices)) == 4:
            quintile_indices = np.vectorize(lambda x: 0 if x == 1 else (1 if x == 2 else x))(quintile_indices)
            
        quintiles = [size_characteristic + "_q" + str(i + 1) for i in quintile_indices]
        x = x.copy()
        x.loc[mask] = quintiles
        return x

    # creating a new dataframe with the quintile labels for each coin based on the size_characteristic for that week
    df["quintile"] = df.groupby(["date"])[size_characteristic].transform(lambda x: q_cut(x, size_characteristic))

    # shifting the quintile information one period into the future
    df["quintile"] = df.groupby("coin_id")["quintile"].shift(1)
    
    # grouping the dataframe by quintile and date
    grouped_df = df.groupby(["quintile", "date"])

    # computing the market_cap-weighted return for each quintile in the following week
    return_df = grouped_df.apply(lambda x: (x["return"] * x["market_cap"]).sum() / x["market_cap"].sum())

    # resetting the index and renaming the columns
    return_df = return_df.reset_index().rename(columns={0:"return"})
    
    # return_df["date"] = return_df["date"].shift(1)

    # pivoting the dataframe to get the return series for each quintile
    return_df = return_df.pivot(index="date", columns="quintile", values="return")

    # adding back NaN values
    date_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq="W")
    return_df = return_df.reindex(date_range)

    return return_df

start_date = pd.to_datetime("2014-01-01")
end_date = pd.to_datetime("today")
directory = r"/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"
weekly_returns_data = pd.read_csv(directory + "/cg_weekly_returns_data.csv", index_col=["date"])
weekly_returns_data.index = pd.to_datetime(weekly_returns_data.index)
risk_free_rate_daily_data = data.fred_data.retrieve_data(start_date, end_date, series_ids = ["DGS1MO"], download=False)
date_range = pd.date_range(start=risk_free_rate_daily_data.index.min(), end=risk_free_rate_daily_data.index.max(), freq="W-SUN")
risk_free_rate = risk_free_rate_daily_data.resample("W-SUN").last().reindex(date_range)
daily_trading_data = pd.read_csv(directory + "/cg_data.csv", index_col=["date"])
daily_trading_data.index = pd.to_datetime(daily_trading_data.index)
print("Computing the excess long-short strategies for the different size characteristics.")
weekly_returns_data["log_market_cap"] = np.log(weekly_returns_data["market_cap"])
weekly_returns_data["log_price"] = np.log(weekly_returns_data["price"])
weekly_returns_data["log_max_price"] = np.log(helpers.convert_frequency(daily_trading_data, method="max")["price"])
weekly_returns_data["age"] = weekly_returns_data.groupby("coin_id")["return"].transform(lambda x: np.maximum(0, (x.index - x.first_valid_index()).days) if x.first_valid_index() is not None else pd.Series([0] * len(x)))
date_range = pd.date_range(start=weekly_returns_data.index.min(), end=weekly_returns_data.index.max(), freq="W-SUN")
weekly_returns_data = weekly_returns_data.groupby("coin_id", group_keys=False).apply(lambda x: x.reindex(date_range))
quintile_data = risk_free_rate
size_characteristics = ["log_market_cap", "log_price", "log_max_price", "age"]
for size_characteristic in size_characteristics:
    quintile_returns_data = quintile_returns(weekly_returns_data, size_characteristic)
    quintile_returns_data = quintile_returns_data.sub(quintile_data["DGS1MO"], axis=0)
    quintile_data = pd.concat([quintile_data, quintile_returns_data], axis=1)
    quintile_data[size_characteristic + "_excess_ls"] = np.where(quintile_data[size_characteristic + "_q1"].isna(), quintile_data[size_characteristic + "_q5"] - quintile_data["DGS1MO"], quintile_data[size_characteristic + "_q5"] - quintile_data[size_characteristic + "_q1"] - quintile_data["DGS1MO"])
