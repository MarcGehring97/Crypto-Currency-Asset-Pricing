import os
path = r"/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"


if os.path.exists(path + "/cg_data.csv"):
    print("Yes")
else:
    print("No")