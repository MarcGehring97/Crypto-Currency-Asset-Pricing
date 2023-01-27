from operator import indexOf
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