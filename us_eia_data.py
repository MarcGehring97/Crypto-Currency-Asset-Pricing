"""
The documentation of the US Energy Information Administration API can be found at https://www.eia.gov/opendata/documentation.php. One can
easily generate the URLs for the GET requests by selecting the appropriate table at https://www.eia.gov/opendata/browser/. There is still
much reformating and preprocessing to do, so it does not make sense to download the data as a csv file at this time.
"""

__all__ = ["retrieve_data"]

def retrieve_data():

    import requests

    api_key = "jLRWwzxhWL7O85sOU5zE6l3FoRtB4FHbOMi1OqQW"

    api_urls = {}
    # the average price data is available only at monthly frequency
    api_urls["average_price"] = "https://api.eia.gov/v2/electricity/retail-sales/data/?api_key=" + api_key + "&frequency=monthly&data[0]=price&start=2011-01&sort[0][column]=customers&sort[0][direction]=desc&offset=0&length=5000"
    api_urls["net_generation"] = "https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/?api_key=" + api_key + "&frequency=daily&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
    api_urls["demand"] = "https://api.eia.gov/v2/electricity/rto/daily-region-sub-ba-data/data/?api_key=" + api_key + "&frequency=daily&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    data = []
    for key in api_urls:
        response = requests.get(api_urls[key])
        print("The API URL for " + key + " is: " + api_urls[key])

        if response.status_code != 200:
            print("There was an error")

        # turn the downloaded data into a dictionary
        data.append((key, response.json()["response"]["data"]))
    
    return data

#print(retrieve_data())