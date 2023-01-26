import pandas as pd, numpy as np, math, time, os, datetime as dt, data.coingecko_data as coingecko_data, helpers, data.fred_data as fred_data
pd.options.mode.chained_assignment = None
from wand.image import Image

# specify the data range for the analysis
# in the paper, the authors start on 2014-01-01 due to data availability
start_date = pd.to_datetime("2014-01-01")
end_date = pd.to_datetime("today")

# select the path to the directory where you want to store the data
directory = r"/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"

# downloading the data from CoinGecko.com and storing it in smaller data subsets at the specified location
# the data contains daily prices, market caps, and trading volumes
# this step can take up to 2 days due to the API traffic limit
# we are also always checking if the subsequent files already exist (/cg_data.csv, /cg_weekly_data.csv, /cg_weekly_returns.csv, /market_weekly_returns.csv)
# this helps in case the previous files have been deleted
files = ["/coingecko", "/cg_data.csv", "/cg_weekly_returns_data.csv", "/market_weekly_returns.csv"]
if any(os.path.exists(directory + file) for file in files):
    # if at least 1 file already exists
    print("The individual data files already exist.")
else:
    # if the file or the subsequent files do not already exist
    coingecko_data.retrieve_data(start_date, end_date, path=directory)

# merging the data subsets and storing the result at the specified location
# this task also absorbs part of the preprocessing, so it's recommended to run this step in any case 
# this step can take long
if any(os.path.exists(directory + file) for file in files[1:]):
    # if at least 1 file already exists
    print("The data was already merged into a single file.")  
else:
    # if the file or the subsequent files do not already exist
    helpers.merge_data(path=directory)  

# the data was retrieved on 2023-01-13
daily_trading_data = pd.read_csv(directory + "/cg_data.csv", index_col=["date"])
daily_trading_data.index = pd.to_datetime(daily_trading_data.index)

# downloading the data since the conversion process might also take a long time
if any(os.path.exists(directory + file) for file in files[2:]):
    # if at least 1 file already exists
    print("The data has already been converted to weekly frequency.")
    weekly_returns_data = pd.read_csv(directory + "/cg_weekly_returns_data.csv", index_col=["date"])
else:
    # this function converts the frequency and also computes the retunrs series
    # this step can take a while
    weekly_returns_data = helpers.convert_frequency(daily_trading_data, method="last", returns=True)
    # some returns are exorbitant (above 10000%)and need to be removed
    weekly_returns_data["return"].where(weekly_returns_data["return"] < 100, np.nan, inplace=True)
    # downloading the data
    weekly_returns_data.to_csv(directory + "/cg_weekly_returns_data.csv")

# computing the risk-free rate
# a pd dataframe with columns for date and DGS1MO
risk_free_rate_daily_data = fred_data.retrieve_data(start_date, end_date, series_ids = ["DGS1MO"], download=False)
# converting to weekly data
date_range = pd.date_range(start=min(risk_free_rate_daily_data.index), end=max(risk_free_rate_daily_data.index), freq="W-SUN")
risk_free_rate = risk_free_rate_daily_data.resample("W-SUN").last().reindex(date_range)

# downloading the data since the returns computation process might also take a long time
if any(os.path.exists(directory + file) for file in files[3:]):
    # if at least 1 file already exists
    print("The market returns data has already been computed.")
    market_weekly_returns = pd.read_csv(directory + "/market_weekly_returns.csv", index_col=["date"])
else:
    # Compute the excess weighted average of the returns using the market capitalization as the weight
    market_weekly_returns = weekly_returns_data.groupby(weekly_returns_data.index).apply(lambda x: (x["return"]*x["market_cap"]).sum()/x["market_cap"].sum())
    market_weekly_returns.rename("market_return", inplace=True)
    market_weekly_returns = market_weekly_returns.to_frame()
    market_weekly_returns.index = pd.to_datetime(market_weekly_returns.index)
    # filling in the missing values with NaNs
    date_range = pd.date_range(start=min(weekly_returns_data.index), end=max(weekly_returns_data.index), freq="W")
    market_weekly_returns = market_weekly_returns.reindex(date_range)
    market_weekly_returns.index.name = "date"
    market_weekly_returns["risk_free_rate"] = risk_free_rate["DGS1MO"].tolist()
    market_weekly_returns["market_return"] = market_weekly_returns["market_return"] - market_weekly_returns["risk_free_rate"]
    market_weekly_returns.drop("risk_free_rate", inplace=True, axis=1)
    # downloading the data
    market_weekly_returns.to_csv(directory + "/market_weekly_returns.csv")

# inverting the colors in the PDF in case the user is using dark mode
if input("Is your editor is dark mode? y/n ") in ["Y", "y"]:
    invert = True
else:
    invert = False

# creates a temporary PDF file named "cover.pdf"
# repeating the process overwrites the file
# we need also the daily data since Panel A is built on daily data
# this step takes about 1 hour
helpers.render_summary_statistics(daily_data=daily_trading_data, market_weekly_data=market_weekly_returns, weekly_data=weekly_returns_data, invert=invert)

pdf_path = os.getcwd() + "/cover.pdf"
# printing the PDF
# this code has to be in the main file
img = Image(filename=pdf_path, resolution=100)
img