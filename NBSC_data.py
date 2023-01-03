"""
Data by the Price Monitoring Center, NDRC was not available. As it turns out, the National Bureau of Statistics of China also does not
provide data about the average price of electricity. If one wishes to use this API, though, one has to face many problems.
"""

# import os
# os.system("pip3 install xarray")
# os.system("pip3 install gb2260")

# example API call
# the following information always need to be provided: indicator code, if the data are regional or national, and a time period
# the Sichuan province has code 510000
# therefore one has to insert "&reg=[{"wdcode":"reg","valuecode":"510000"}]" into the code
import requests, pandas as pd
api_url = 'http://data.stats.gov.cn/english/easyquery.htm?m=QueryData&dbcode=fsnd&rowcode=reg&colcode=sj&wds=[{"wdcode":"zb","valuecode":"A090201"}]&dfwds=[{"wdcode":"sj","valuecode":"2011-"}]&k1=1472740901192'

response = requests.get(api_url, verify=False)

# turn the downloaded data into a dictionary
meta_info = response.json()["returndata"]["wdnodes"]
data = response.json()["returndata"]["datanodes"]

# this code can be used to find the indicator code of a data series
# every data set has an indicator starting with "A0", followed by 5 to 7 numbers
# there are several different data bases and it is not clear in what data base the desired data series is stored
# I have identified the following data bases: hgyd, hgjd, hgnd, fsyd, fsjd, fsnd, csyd, csnd, gjyd, gjydsdj, gjydsc, gjnd, gatyd, gatnd
# the description can be found in the website browser at https://data.stats.gov.cn/english/index.htm
def find_indicator(data_base = "hgyd", description = "Output of Electricity, Current Period(100 million kwh)"):
    i = 0
    indicator = ""
    while i <= 99999:
        index = "A0" + str(i).zfill(5)
        print(index)
        api_url = "https://data.stats.gov.cn/english/easyquery.htm?m=QueryData&dbcode=" + data_base + "&rowcode=reg&colcode=sj&wds=[{%22wdcode%22:%22zb%22,%22valuecode%22:%22" + index + "%22}]&dfwds=[{%22wdcode%22:%22sj%22,%22valuecode%22:%22%22}]&k1=" + str(int(datetime.datetime.now().timestamp() * 1000))
        try:
            response = requests.get(api_url, verify=False)
        except:
            i += 1
            continue
        
        if response.status_code != 200:
            print("There was an error")
            i += 1
            continue

        try:
            meta_info = response.json()["returndata"]["wdnodes"]
            name = meta_info[0]["nodes"][0]["cname"]
            print(name)
        except:
            i += 1
            continue

        if name == description:
            return "The indicator ID is: " + index

        i += 1

    print("The indicator ID could not be found.")
