import pandas as pd, numpy as np

a = pd.DataFrame({"test": []})

a["test"] = [0.0, 0.0, 3]

a["test"] = a["test"].replace(0.0, np.nan)

print(a)