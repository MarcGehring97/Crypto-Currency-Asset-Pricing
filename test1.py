

import pandas as pd, numpy as np

num = 10

date_range = pd.date_range(start='2021-01-01', end='2023-12-31', freq='D').repeat(num)

data = {'date': date_range, 
        'price': np.random.normal(5, 50, num * 365 * 3),
        'return': np.random.normal(0.5, 0.5, num * 365 * 3),
        'market_return': np.random.normal(0.5, 0.5, num * 365 * 3),
        'market_cap': np.random.randint(1000000, 5000000000, num * 365 * 3),
        'total_volume': np.random.randint(10000, 500000, num * 365 * 3),                          
        'size_characteristic': np.random.normal(100, 50, num * 365 * 3)}
        
coin_id = []
for j in range(365 * 3):
    for i in range(num):                            
        coin_id += [i]
    
data["coin_id"] = coin_id
                            
df = pd.DataFrame(data)

# Replace random values in price, market_cap, and size_characteristic columns with missing values
cols = ['price', 'market_cap', 'size_characteristic']
for col in cols:
    mask = np.random.rand(len(df)) < 0.0
    df[col][mask] = np.nan

df = df.sort_values(by=["coin_id", "date"])

df.set_index("date", inplace=True, drop=True)

print(df.head(50))
df["market_cap"].where(df["market_cap"] < 1e+08, np.nan, inplace=True)
print(df.head(50))