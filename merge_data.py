"""
This file can be used to merge several downloaded data frames into one large data set. The user has to
indicate the correct path destination where the data subsets are stored.
"""

import pandas as pd
import os

# reading all data subsets as data frames
file_names = os.listdir('dir_path')
path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"
data = pd.DataFrame()
for file_name in file_names:
    data.append(pd.read_csv(path + "/" + file_name))

data.to_csv(path + "/cg_data.csv", index=False)
