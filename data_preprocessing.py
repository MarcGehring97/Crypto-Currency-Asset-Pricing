import blockchain_com_data, pandas as pd


path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"
path_coingecko = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data/coingecko"
charts = ["n-unique-addresses", "n-transactions"]
# charts.append("n-payments")
# the API for the chart n-payments is currently not available

data = blockchain_com_data.retrieve_data(path=path, charts=charts, download=False)
