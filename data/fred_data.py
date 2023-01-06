"""
The IDs of the series can be found on the sites of the series themselves. They are also the name of the name of the path

The function "retrieve_data" returns a pd dataframe with columns for date, DEXUSAL, DEXCAUS, DEXUSEU, DEXSIUS, DEXUSUK
"""

__all__ = ["retrieve_data"]

def retrieve_data(series_ids=["DEXUSAL", "DEXCAUS", "DEXUSEU", "DEXSIUS", "DEXUSUK"]):

    import requests

    api_key = "f32180d669b6c6eccde4ddfba4c49a7c"
    
    data = []
    for series_id in series_ids:
        api_url = "https://api.stlouisfed.org/fred/series?series_id=" + series_id + "&file_type=json&realtime_start=2011-01-01&api_key=" + api_key + ""

        if response.status_code != 200:
            print("There was an error")
            continue

        response = requests.get(api_url)

        # turn the downloaded data into a dictionary
        data.append((series_id, response.json()["response"]["data"]))
    