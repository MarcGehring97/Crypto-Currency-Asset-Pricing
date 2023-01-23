"""
In order to download the data series for number of tweets containing the word "Bitcoin" one has to get a bearer_token from Twitter by applying
for the Academic Research access to the Twitter API. One can do that here at https://developer.twitter.com/en/products/twitter-api/academic-research.
The user will have to use the function "counts_all" specified at https://developer.twitter.com/en/docs/twitter-api/tweets/counts/quick-start/full-archive-tweet-counts.
The official code example can be found at https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/Full-Archive-Tweet-Counts/full_archive_tweet_counts.py
but I have noticed that the code here https://github.com/twitterdev/getting-started-with-the-twitter-api-v2-for-academic-research/blob/main/labs-code/python/academic-research-product-track/full_archive_tweet_counts.py
is more useful. It uses the library "twarc" (https://twarc-project.readthedocs.io/en/latest/) for authentication. The repository contains more
useful information. The downloading and processing might take a couple of minutes to complete.

The function "retrieve_data" has the following arguments:
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- query: The terms the user wants the count of tweets containing them per day of. The default is "Bitcoin".
- download: Whether the user wants to download the data or get them returned. The default is True.
- bearer_token: The bearer token the user needs to authenticate. The default is "".

The function "retrieve_data" returns a pd dataframe with columns for date and tweet_count
"""

__all__ = ["retrieve_data"]

def retrieve_data(start_date, end_date, path="", query=["Bitcoin"], download=True, bearer_token=""):

    from twarc import Twarc2
    import datetime, pandas as pd, os

    if path != "":
        file_names = os.listdir(path)

    client = Twarc2(bearer_token=bearer_token)

    # specify the start time in UTC for the time period you want Tweets from

    # today does not work so it has to be the day before
    start_date = start_date
    end_date = end_date - datetime.timedelta(days=1)

    # the counts_all method call the full-archive tweet counts endpoint to get tweet volume based on the query, start, and end times
    count_results = client.counts_all(query=query, start_time=start_date, end_time=end_date, granularity="day")

    # iterating through and processing the data
    dates = []
    data = []
    for page in count_results:
        page_data = page["data"]
        for data_point in reversed(page_data):
            dates.append(pd.to_datetime(data_point["end"]).date())
            data.append(data_point["tweet_count"])
    
    df = pd.DataFrame.from_dict({"date": dates, "tweet_count": data})
    df = df.reindex(index=df.index[::-1])
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    df = df.drop_duplicates(subset="date")
    df.set_index("date", inplace=True, drop=True)
    df = df.reindex(date_range)

    if download:
        if "twitter_data.csv" not in file_names:
            df.to_csv(path + "/twitter_data.csv")
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/twitter_data.csv")
                df.to_csv(path + "/twitter_data.csv")
            else:
                print("Could not create a new file.")
    else: 
        return df

import pandas as pd, json
bearer_token = json.load(open("/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/twitter_bearer_token_main.json"))["bearer_token"]
print(retrieve_data(start_date=pd.to_datetime("2014-01-01"), end_date=pd.to_datetime("today"), bearer_token=bearer_token, download=False).head(50))
