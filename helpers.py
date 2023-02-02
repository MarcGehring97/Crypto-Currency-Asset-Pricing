"""
The "merge" function can be used to merge several downloaded data frames into one large data set. The user has to indicate the correct path destination 
where the data subsets are stored. The render functions can be used to create a PDF of the different tables and graphs found in the paper. Each function 
takes in the required data, computes the statistics or time series, and inserts them in the appropriate LaTeX files to ultimately create a PDF file. 
The PDF file can be shown in the main.ipynb file.

The function "merge" has the following arguments:
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "merge" returns a pd dataframe with columns for id, date, price, market_cap, and total_volume.

The function "convert_frequency" has the following arguments:
- data: The data series to convert the frequency for.
- method: The method of choosing the data for the weekly data series.
- returns: Whether or not to compute the returns series, as well. Per default, this does not happen.

The function "convert_frequency" returns a pd dataframe with the columns it was given. Optionally, the function adds a column "return".

The function "render_summary_statistics" has the following arguments:
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- daily_trading_data: This data is needed to compute the statistics for Bitcoin, Ethereum, and Ripple in Panel B.
- market_weekly_returns: This data is needed to compute the statistics for the market returns in Panel B.
- coins_weekly_returns: This data is needed to compute the statistics for each year in Panel A.
- invert: whether or not the PDF should be color-inverted (because of dark mode).

The function "render_summary_statistics" adds the PDF file "cover.pdf" that can be rendered below the code in the main.ipynb file.

The function "render_size_strategy_returns" has the following arguments:
- long_short_data: This data is needed to compute the statistics.
- invert: whether or not the PDF should be color-inverted (because of dark mode).

The function "render_size_strategy_returns" adds the PDF file "cover.pdf" that can be rendered below the code in the main.ipynb file.

The function "render_momentum_strategy_returns" has the following arguments:
- long_short_data: This data is needed to compute the statistics.
- invert: whether or not the PDF should be color-inverted (because of dark mode).

The function "render_momentum_strategy_returns" adds the PDF file "cover.pdf" that can be rendered below the code in the main.ipynb file.
"""

__all__ = ["merge_data", "convert_frequency", "quintile_returns", "tertile_returns", "two_times_three_returns", "render_summary_statistics", "render_quintiles", "render_one_factor_model_statistics", "render_multi_factor_models_statistics"]

import pandas as pd, os, subprocess, easydict, time, numpy as np

# reading all data subsets as data frames
def merge_data(path="", download=True):

    file_names = os.listdir(path + "/coingecko")

    dfs = []
    for file_name in file_names:
        if file_name[-4:] == ".csv" and file_name[-11:] != "cg_data.csv":
            df = pd.read_csv(path + "/coingecko" + "/" + file_name, index_col=["date"])
            dfs.append(df)

    # combining all dataframes in the dfs list
    output = pd.concat(dfs)
    
    if download:
        if "cg_data.csv" not in file_names:
            output.to_csv(path + "/cg_data.csv")
        else:
            if input("The file already exists. Do you want to replace it? Y/N ") == "Y":
                os.remove(path + "/cg_data.csv")
                output.to_csv(path + "/cg_data.csv")
            else:
                print("Could not create a new file.")
    else:
        return output

# import datetime
# merge(start_date="2014-01-01", end_date="2023-01-12",path=r"/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data")

def convert_frequency(data, method, returns=False):
    
    date_range = pd.date_range(start=min(data.index), end=max(data.index), freq="W-SUN")
    
    if method == "last":
        data = data.groupby("coin_id").apply(lambda x: x.resample("W-SUN").last().reindex(date_range)).reset_index(level="coin_id", drop=True)
    elif method == "max":
        data = data.groupby("coin_id").apply(lambda x: x.resample("W-SUN").max().reindex(date_range)).reset_index(level="coin_id", drop=True)
    elif method == "mean":
        data = data.groupby("coin_id").apply(lambda x: x.resample("W-SUN").mean().reindex(date_range)).reset_index(level="coin_id", drop=True)
    
    # .pct_change() defaults to 0 for missing data
    if returns:
        data["return"] = data.groupby("coin_id")["price"].pct_change()

    data.index.name = "date"
    
    return data

# function for the zero-investment long-short strategy
def quintile_returns(df, var):

    # adapting the Pandas qcut function for the edge cases
    def q_cut(x, var):

        if x.isna().all():
            return pd.Series(np.nan * len(x))

        # this step is necessary since qcut sometimes assigns fewer categorites to the data than specified
        # Compute quantiles for all non-missing values
        mask = x.notna()        
        # computing quintiles manually since the Pandas's qcut() results in too many complications
        quintile_indices = np.digitize(x[mask], np.percentile(x[mask], [20, 40, 60, 80]), right=False)
        # mapping the quintiles to the appropriate quintile values 
        if len(np.unique(quintile_indices)) == 4:
            quintile_indices = np.vectorize(lambda x: 0 if x == 1 else (1 if x == 2 else x))(quintile_indices)
        if len(np.unique(quintile_indices)) == 3:
            quintile_indices = np.vectorize(lambda x: 0 if x == 2 else (2 if x == 3 else x))(quintile_indices)
        elif len(np.unique(quintile_indices)) == 2:
            quintile_indices = np.vectorize(lambda x: 0 if x == 3 else x)(quintile_indices)
            
        quintiles = [var + "_q" + str(i + 1) for i in quintile_indices]
        x = x.copy()
        x.loc[mask] = quintiles
        return x

    # creating a new dataframe with the quintile labels for each coin based on the var for that week
    df["quintile"] = df.groupby(["date"])[var].transform(lambda x: q_cut(x, var))

    # shifting the quintile information one period into the future
    df["quintile"] = df.groupby("coin_id")["quintile"].shift(1)
    
    # grouping the dataframe by quintile and date
    grouped_df = df.groupby(["quintile", "date"])

    # computing the market_cap-weighted return for each quintile in the following week
    return_df = grouped_df.apply(lambda x: (x["return"] * x["market_cap"]).sum() / x["market_cap"].sum())

    # resetting the index and renaming the columns
    return_df = return_df.reset_index().rename(columns={0:"return"})
    
    # return_df["date"] = return_df["date"].shift(1)

    # pivoting the dataframe to get the return series for each quintile
    return_df = return_df.pivot(index="date", columns="quintile", values="return")

    # adding back NaN values
    date_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq="W")
    return_df = return_df.reindex(date_range)

    return return_df

def tertile_returns(df, var):

    # adapting the Pandas qcut function for the edge cases
    def q_cut(x, var):

        if x.isna().all():
            return pd.Series(np.nan * len(x))

        # this step is necessary since qcut sometimes assigns fewer categorites to the data than specified
        # Compute quantiles for all non-missing values
        mask = x.notna()        
        # computing tertile manually since the Pandas's qcut() results in too many complications
        tertile_indices = np.digitize(x[mask], np.percentile(x[mask], [30, 70]), right=False)
        # mapping the tertile to the appropriate tertile values
        if len(np.unique(tertile_indices)) == 2:
            tertile_indices = np.vectorize(lambda x: 0 if x == 1 else x)(tertile_indices)
            
        tertile = [var + "_q" + str(i + 1) for i in tertile_indices]
        x = x.copy()
        x.loc[mask] = tertile
        return x

    # creating a new dataframe with the tertile labels for each coin based on the var for that week
    df["tertile"] = df.groupby(["date"])[var].transform(lambda x: q_cut(x, var))

    # here, we don't shift the quantiles 1 week into the future
    
    # grouping the dataframe by tertile and date
    grouped_df = df.groupby(["tertile", "date"])

    # computing the market_cap-weighted return for each tertile in the same week
    return_df = grouped_df.apply(lambda x: (x["return"] * x["market_cap"]).sum() / x["market_cap"].sum())

    # resetting the index and renaming the columns
    return_df = return_df.reset_index().rename(columns={0:"return"})
    
    # return_df["date"] = return_df["date"].shift(1)

    # pivoting the dataframe to get the return series for each tertile
    return_df = return_df.pivot(index="date", columns="tertile", values="return")

    # adding back NaN values
    date_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq="W")
    return_df = return_df.reindex(date_range)

    return return_df

def two_times_three_returns(df):

    # adapting the Pandas qcut function for the edge cases
    def q_cut(x):

        if x.isna().all():
            return pd.Series(np.nan * len(x))

        # this step is necessary since qcut sometimes assigns fewer categorites to the data than specified
        # Compute quantiles for all non-missing values
        mask = x.notna()        
        # computing the median
        median_indices = np.digitize(x[mask], np.percentile(x[mask], [50]), right=False)
            
        median = ["market_cap_q" + str(i + 1) for i in median_indices]
        x = x.copy()
        x.loc[mask] = median
        return x

    # creating a new dataframe with the median labels for each coin based on the market_cap for that week
    df["median"] = df.groupby(["date"])["market_cap"].transform(lambda x: q_cut(x))

    # here, we don't shift the quantiles 1 week into the future

    # dividing the data set by quantile
    q1_return_df = tertile_returns(df[df["median"] == "q1"], "three_week_momentum")
    q2_return_df = tertile_returns(df[df["median"] == "q2"], "three_week_momentum")

    return (q1_return_df, q2_return_df)

def render_summary_statistics(daily_data, market_weekly_data, weekly_data, invert):
    
    # the number of coins per year for which not every return data point is missing
    number_of_coins = daily_data.groupby(daily_data.index.year).apply(lambda x: x.groupby("coin_id").apply(lambda x: x["price"].notna().sum() > 0).sum())

    # the other statistics
    # group the data by date
    grouped = daily_data.groupby(daily_data.index.year)
    grouped_stats = grouped[["market_cap", "total_volume"]].agg(["mean", "median"])
    mean_market_caps = grouped_stats.loc[:, ("market_cap", "mean")]
    median_market_caps = grouped_stats.loc[:, ("market_cap", "median")]
    mean_volumes = grouped_stats.loc[:, ("total_volume", "mean")]
    median_volumes = grouped_stats.loc[:, ("total_volume", "median")]
    
    # opens the LaTeX summary statistics table template as a string
    template = open("latex_templates/summary_statistics.tex", "r").read()

    # adding the rows to the LaTeX template
    panel_a_rows = ""
    for year in daily_data.index.year.unique().dropna():
        panel_a_rows += f"{year} & {number_of_coins[number_of_coins.index == year].tolist()[0]} & {round(mean_market_caps[mean_market_caps.index == year].tolist()[0] / 1000000, 2):,} & {round(median_market_caps[median_market_caps.index == year].tolist()[0] / 1000000, 2):,} & {round(mean_volumes[mean_volumes.index == year].tolist()[0] / 1000, 2):,} & {round(median_volumes[median_volumes.index == year].tolist()[0] / 1000, 2):,} \\\ "

    panel_a_summary = f"{len(daily_data['coin_id'].unique().sum())} & {round(np.mean(daily_data['market_cap']) / 1000000, 2):,} & {round(np.median(daily_data['market_cap']) / 1000000, 2):,} & {round(np.mean(daily_data['total_volume']) / 1000, 2):,} & {round(np.median(daily_data['total_volume']) / 1000, 2):,}"
    panel_b_market_return = f"{round(np.mean(market_weekly_data['market_excess_return'].dropna()), 3):,} & {round(market_weekly_data['market_excess_return'].dropna().median(), 3):,} & {round(np.std(market_weekly_data['market_excess_return'].dropna()), 3):,} & {round(market_weekly_data['market_excess_return'].dropna().skew(), 3):,} & {round(market_weekly_data['market_excess_return'].dropna().kurtosis(), 3):,}"
    panel_b_bitcoin_return = f"{round(weekly_data[weekly_data['coin_id'] == 'bitcoin']['return'].dropna().mean(), 3):,} & {round(weekly_data[weekly_data['coin_id'] == 'bitcoin']['return'].dropna().median(), 3):,} & {round(weekly_data[weekly_data['coin_id'] == 'bitcoin']['return'].dropna().std(), 3):,} & {round(weekly_data[weekly_data['coin_id'] == 'bitcoin']['return'].dropna().skew(), 3):,} & {round(weekly_data[weekly_data['coin_id'] == 'bitcoin']['return'].dropna().kurtosis(), 3):,}"
    panel_b_ripple_return = f"{round(weekly_data[weekly_data['coin_id'] == 'ethereum']['return'].dropna().mean(), 3):,} & {round(weekly_data[weekly_data['coin_id'] == 'ethereum']['return'].dropna().median(), 3):,} & {round(weekly_data[weekly_data['coin_id'] == 'ethereum']['return'].dropna().std(), 3):,} & {round(weekly_data[weekly_data['coin_id'] == 'ethereum']['return'].dropna().skew(), 3):,} & {round(weekly_data[weekly_data['coin_id'] == 'ethereum']['return'].dropna().kurtosis(), 3):,}"
    panel_b_ethereum_return = f"{round(weekly_data[weekly_data['coin_id'] == 'ripple']['return'].dropna().mean(), 3):,} & {round(weekly_data[weekly_data['coin_id'] == 'ripple']['return'].dropna().median(), 3):,} & {round(weekly_data[weekly_data['coin_id'] == 'ripple']['return'].dropna().std(), 3):,} & {round(weekly_data[weekly_data['coin_id'] == 'ripple']['return'].dropna().skew(), 3):,} & {round(weekly_data[weekly_data['coin_id'] == 'ripple']['return'].dropna().kurtosis(), 3):,}"

    template = template.replace("<Panel A rows>", panel_a_rows)
    template = template.replace("<Panel A summary>", panel_a_summary)
    template = template.replace("<Panel B market return>", panel_b_market_return)
    template = template.replace("<Panel B Bitcoin return>", panel_b_bitcoin_return)
    template = template.replace("<Panel B Ripple return>", panel_b_ripple_return)
    template = template.replace("<Panel B Ethereum return>", panel_b_ethereum_return)

    args = easydict.EasyDict({})

    with open("cover.tex", "w") as f:
        f.write(template%args.__dict__)

    cmd = ["pdflatex", "-interaction", "nonstopmode", "cover.tex"]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    proc.communicate()

    retcode = proc.returncode
    if not retcode == 0:
        os.unlink("cover.pdf")
        raise ValueError("Error {} executing command: {}".format(retcode, " ".join(cmd))) 

    os.unlink("cover.tex")
    os.unlink("cover.log")
    os.unlink("cover.aux")

    time.sleep(3)
    # the path where the PDF is stored
    pdf_path = os.getcwd() + "/cover.pdf"

    # inverting the colors in the PDF in case the user is using dark mode
    if invert:

        from pdf2image import convert_from_path
        from PIL import ImageChops

        pil_image_lst = convert_from_path(pdf_path)
        image = pil_image_lst[0]
        image = ImageChops.invert(image)
        image.save(pdf_path)
        time.sleep(3)

def render_quintiles(data, template, variables, invert):

    from scipy.stats import ttest_1samp

    # opens the LaTeX quintile table template as a string
    template = open(template, "r").read()

    for var in variables:
        mean_row = ""
        t_row = ""
        for quintile in ["_q1", "_q2", "_q3", "_q4", "_q5", "_long_short"]:
            column = data[var + quintile]
            # computing the statistics
            mean = column.mean(skipna=True)
            # per default, this function performs a two-sided t-test
            t_statstic, p_value = ttest_1samp(column, 0, nan_policy="omit")
            asterisk = ""
            match p_value:
                case _ if p_value <= 0.01:
                    asterisk = "***"
                case _ if p_value <= 0.05:
                    asterisk = "**"
                case _ if p_value <= 0.1:
                    asterisk = "*"
            mean_row += " & " + str(round(mean, 3)) + asterisk + ""
            t_row += " & (" + str(round(t_statstic, 2)) + ")"
                
        template = template.replace("<" + var + "_mean>", mean_row)
        template = template.replace("<" + var + "_t>", t_row)

    args = easydict.EasyDict({})

    with open("cover.tex", "w") as f:
        f.write(template%args.__dict__)

    cmd = ["pdflatex", "-interaction", "nonstopmode", "cover.tex"]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    proc.communicate()

    retcode = proc.returncode
    if not retcode == 0:
        os.unlink("cover.pdf")
        raise ValueError("Error {} executing command: {}".format(retcode, " ".join(cmd))) 

    os.unlink("cover.tex")
    os.unlink("cover.log")
    os.unlink("cover.aux")

    time.sleep(3)
    # the path where the PDF is stored
    pdf_path = os.getcwd() + "/cover.pdf"

    # inverting the colors in the PDF in case the user is using dark mode
    if invert:

        from pdf2image import convert_from_path
        from PIL import ImageChops

        pil_image_lst = convert_from_path(pdf_path)
        image = pil_image_lst[0]
        image = ImageChops.invert(image)
        image.save(pdf_path)
        time.sleep(3)

def render_one_factor_model_statistics(data, template, variables, invert):

    # opens the LaTeX factor model statistics table template as a string
    template = open(template, "r").read()

    row = ""
    for var in variables:
        row_data = data[data["ls_strategy"] == var]
        row += " & " + str(round(row_data["alpha"], 3))
        # computing the asterisks for the alphas
        asterisk_alpha = ""
        match row_data["p_alpha"]:
            case _ if row_data["p_alpha"] <= 0.01:
                asterisk_alpha = "***"
            case _ if row_data["p_alpha"] <= 0.05:
                asterisk_alpha = "**"
            case _ if row_data["p_alpha"] <= 0.1:
                asterisk_alpha = "*"
        row += asterisk_alpha + " & (" + str(round(row_data["t_alpha"], 2)) + ") & " + str(round(row_data["beta"], 3))
        # computing the asterisks for the betas
        asterisk_beta = ""
        match row_data["p_beta"]:
            case _ if row_data["p_beta"] <= 0.01:
                asterisk_beta = "***"
            case _ if row_data["p_beta"] <= 0.05:
                asterisk_beta = "**"
            case _ if row_data["p_beta"] <= 0.1:
                asterisk_beta = "*"
        row += asterisk_beta + " & (" + str(round(row_data["t_beta"], 2)) + ") & " + str(round(row_data["r_squared"], 3)) + " & " + str(round(row_data["mean_absolute_pricing_error"], 3)) + " & " + str(round(row_data["average_r_squared"], 3))
                
        template = template.replace("<" + var + "_data>", row)

    args = easydict.EasyDict({})

    with open("cover.tex", "w") as f:
        f.write(template%args.__dict__)

    cmd = ["pdflatex", "-interaction", "nonstopmode", "cover.tex"]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    proc.communicate()

    retcode = proc.returncode
    if not retcode == 0:
        os.unlink("cover.pdf")
        raise ValueError("Error {} executing command: {}".format(retcode, " ".join(cmd))) 

    os.unlink("cover.tex")
    os.unlink("cover.log")
    os.unlink("cover.aux")

    time.sleep(3)
    # the path where the PDF is stored
    pdf_path = os.getcwd() + "/cover.pdf"

    # inverting the colors in the PDF in case the user is using dark mode
    if invert:

        from pdf2image import convert_from_path
        from PIL import ImageChops

        pil_image_lst = convert_from_path(pdf_path)
        image = pil_image_lst[0]
        image = ImageChops.invert(image)
        image.save(pdf_path)
        time.sleep(3)

def render_multi_factor_models_statistics(data, template, variables, invert):

    # opens the LaTeX factor model statistics table template as a string
    template = open(template, "r").read()

    row = ""
    for var in variables:
        row_data = data[data["ls_strategy"] == var]
        row += " & " + str(round(row_data["alpha"], 3))
        # computing the asterisks for the alphas
        asterisk_alpha = ""
        match row_data["p_alpha"]:
            case _ if row_data["p_alpha"] <= 0.01:
                asterisk_alpha = "***"
            case _ if row_data["p_alpha"] <= 0.05:
                asterisk_alpha = "**"
            case _ if row_data["p_alpha"] <= 0.1:
                asterisk_alpha = "*"
        row += asterisk_alpha + " & (" + str(round(row_data["t_alpha"], 2)) + ") & " + str(round(row_data["beta"], 3))
        # computing the asterisks for the betas
        asterisk_beta = ""
        match row_data["p_beta"]:
            case _ if row_data["p_beta"] <= 0.01:
                asterisk_beta = "***"
            case _ if row_data["p_beta"] <= 0.05:
                asterisk_beta = "**"
            case _ if row_data["p_beta"] <= 0.1:
                asterisk_beta = "*"
        row += asterisk_beta + " & (" + str(round(row_data["t_beta"], 2)) + ") & " + str(round(row_data["r_squared"], 3)) + " & " + str(round(row_data["mean_absolute_pricing_error"], 3)) + " & " + str(round(row_data["average_r_squared"], 3))
                
        template = template.replace("<" + var + "_data>", row)

    args = easydict.EasyDict({})

    with open("cover.tex", "w") as f:
        f.write(template%args.__dict__)

    cmd = ["pdflatex", "-interaction", "nonstopmode", "cover.tex"]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    proc.communicate()

    retcode = proc.returncode
    if not retcode == 0:
        os.unlink("cover.pdf")
        raise ValueError("Error {} executing command: {}".format(retcode, " ".join(cmd))) 

    os.unlink("cover.tex")
    os.unlink("cover.log")
    os.unlink("cover.aux")

    time.sleep(3)
    # the path where the PDF is stored
    pdf_path = os.getcwd() + "/cover.pdf"

    # inverting the colors in the PDF in case the user is using dark mode
    if invert:

        from pdf2image import convert_from_path
        from PIL import ImageChops

        pil_image_lst = convert_from_path(pdf_path)
        image = pil_image_lst[0]
        image = ImageChops.invert(image)
        image.save(pdf_path)
        time.sleep(3)