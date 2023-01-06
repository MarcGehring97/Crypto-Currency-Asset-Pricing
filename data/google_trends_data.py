"""
The pytrends documentation can be found at https://pypi.org/project/pytrends/. The repository can be found at
https://github.com/GeneralMills/pytrends/tree/master/pytrends. Regrettably, the data is available at a monthly frequency only.

The function "retrieve_data" has the following arguments:
- path: The path where the user intends to store the data. The default is "".
- kw_list: A list of all search keywords for which the time series should be downloaded or returned. The default is the keyword "Bitcoin".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "retrieve_data" returns a pd dataframe with columns for date and search_count
"""

__all__ = ["retrieve_data"]

def retrieve_data(path="", kw_list=["Bitcoin"], download=True):

    import os, sys, datetime
    
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    sys.path.append(PROJECT_ROOT)
    # so that this module can find pytrends

    from pytrends.request import TrendReq

    pytrend = TrendReq()

    today = str(datetime.date.today())
    timeframe = "2011-01-01 " + today

    pytrend.build_payload(kw_list, timeframe=timeframe)

    # interest represents search interest relative to the highest point on the chart for the given region and time
    # a value of 100 is the peak popularity for the term
    interest = pytrend.interest_over_time()
    interest = interest.reset_index(level=0)
    interest = interest.drop("isPartial", axis=1)
    interest = interest.rename(columns={"Bitcoin": "search_count"})

    if path != "":
        file_names = os.listdir(path)

    if download:
        if "google_trends_data.csv" not in file_names:
            interest.to_csv(path + "/google_trends_data.csv", index=False)
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/google_trends_data.csv")
                interest.to_csv(path + "/google_trends_data.csv", index=False)
            else:
                print("Could not create a new file.")
    else: 
        return interest

print(retrieve_data(download=False).head())
#4388