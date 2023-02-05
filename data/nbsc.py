"""
Data by the Price Monitoring Center, NDRC was not available. As it turns out, the National Bureau of Statistics of China also does not
provide data about the average price of electricity. Right now, the function "retrieve_data" returns monthly data on "Output of Electricity, 
Current Period(100 million kwh)". If one wants to retrieve data for the Sichuan province only, he/she should look into the following two
repositories to understand how to change the API URLs below: https://github.com/khaeru/data/blob/master/cn_nbs.py and https://github.com/mbk-dev/nbsc.
The Sichuan province has code 510000. Therefore, one probably has to insert "&reg=[{"wdcode":"reg","valuecode":"510000"}]" into the URL. 
This code might be incomplete since I do not use this data. I also did not check if all days are included in the data for the specified data 
range. One can get the needed code from the file google_trends_data.py, for exmaple. Also, apparently, the URLs get constantly updated, but 
I think there are also ways to create permanently working URLs. It is best to look at one of the two mentioned GitHub repositories.

The function "retrieve_data" has the following arguments:
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.
"""

__all__ = ["retrieve_data"]

def retrieve_data(path="", download=True):
    import requests, pandas as pd, os
    if path != "":
        file_names = os.listdir(path)
    # the website browser can be found at https://data.stats.gov.cn/english/index.htm
    # there, one can also see the format for the date range to be specifical in the API URL
    # if one wishes to find the API URL of another data series, one has to open the developer tool, go to network, then Fetch/XHR, navigate to the desired data series in the browser, and then inspect the different queries
    # for URL encoding: https://de.wikipedia.org/wiki/URL-Encoding
    api_url = "https://data.stats.gov.cn/english/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=zb&colcode=sj&wds=%5B%5D&dfwds=%5B%7B%22wdcode%22%3A%22zb%22%2C%22valuecode%22%3A%22A03010G%22%7D%5D&k1=1672874627369"
    # the value code for there data series here is A03010G
    print("The API URL is: " + api_url + ".")
    response = requests.get(api_url, verify=False)
    meta_info = response.json()["returndata"]["wdnodes"]
    name = meta_info[0]["nodes"][0]["cname"]
    print(name)
    response = response.json()
    data = []
    dates = []
    i = 0
    while True:
        dates.append(int(response["returndata"]["datanodes"][i]["wds"][1]["valuecode"]))
        data.append(response["returndata"]["datanodes"][i]["data"]["data"])
        if response["returndata"]["datanodes"][i]["wds"][1]["valuecode"] == "201101":
            break
        i += 1
    df = pd.DataFrame.from_dict({"dates": dates, "Output of Electricity, Current Period(100 million kwh)": data})
    df["date"] = pd.to_datetime(data["date"])
    df = df.drop_duplicates(subset="date")
    df.set_index("date", inplace=True, drop=True)
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    df = df.reindex(date_range)
    if download:
        if "nbsc_data.csv" not in file_names:
            df.to_csv(path + "/nbsc_data.csv")
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove("/nbsc_data.csv")
                df.to_csv(path + "/nbsc_data.csv")
            else:
                print("Could not create a new file.")
    else: 
        return df