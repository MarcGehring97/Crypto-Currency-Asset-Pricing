"""
This file can be used to merge several downloaded data frames into one large data set. The user has to
indicate the correct path destination where the data subsets are stored. The output is then stored in
the same directory.

The function "merge" has the following arguments:
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.
"""

# reading all data subsets as data frames
def merge(path="", download=True):
    import pandas as pd, os

    file_names = os.listdir(path)
    dfs = []
    for file_name in file_names:
        if file_name[-4:] == ".csv" and file_name[-11:] != "cg_data.csv":
            df = pd.read_csv(path + "/" + file_name)
            dfs.append(df)

    # combining all dataframes in the dfs list
    data = pd.concat(dfs)
            
    # removing any duplicate rows
    data = data.drop_duplicates()

    if download:
        if "cg_data.csv" not in file_names:
            data.to_csv(path + "/cg_data.csv", index=False)
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/cg_data.csv")
                data.to_csv(path + "/cg_data.csv", index=False)
            else:
                print("Could not create a new file.")
    else:
        return data

# merge()