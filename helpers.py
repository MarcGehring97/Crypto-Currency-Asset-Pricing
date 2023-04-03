"""
The function "merge" has the following arguments:
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.
The function "merge" returns a pd dataframe with columns for id, date, price, market_cap, and total_volume.

The function "convert_frequency" has the following arguments:
- data: The data series to convert the frequency for.
- method: The method of choosing the data for the weekly data series.
- returns: Whether or not to compute the returns series, as well. Per default, this does not happen.
The function "convert_frequency" returns a pd dataframe with the columns it was given. Optionally, the function adds a column "return".

The function "quintile_returns" has the following arguments:
- df: The data to compute the quintiles for.
- var: The variables to compute the quintiles for.
The function "quintile_returns" return a pd dataframe with columns for all 5 quintile return series.

The function "tertile_returns" has the following arguments:
- df: The data to compute the tertiles for.
- var: The variables to compute the tertiles for.
The function "tertile_returns" return a pd dataframe with columns for all 3 tertiles return series.

The function "two_times_three_returns" has the following arguments:
- df: The data to compute the tertiles for.
The function "two_times_three_returns" returns 2 pd dataframes, one for each market_cap quantile, each with columns for 3 tertiles for three_week_momentum.

The function "render_summary_statistics" has the following arguments:
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- daily_trading_data: This data is needed to compute the statistics for Bitcoin, Ethereum, and Ripple in Panel B.
- market_weekly_returns: This data is needed to compute the statistics for the market returns in Panel B.
- coins_weekly_returns: This data is needed to compute the statistics for each year in Panel A.
- invert: whether or not the PDF should be color-inverted (because of dark mode).
The function "render_summary_statistics" adds the PDF file "cover.pdf" that can be rendered below the code in the main.ipynb file.

The function "render_quintiles" has the following arguments:
- data: This data is needed to render the quintiles for.
- template: The respective LaTeX template found in the latex_templates folder.
- variables: The variables to render the quintiles for.
- invert: Whether or not the PDF should be color-inverted (because of dark mode).
The function "render_quintiles" adds the PDF file "cover.pdf" that can be rendered below the code in the main.ipynb file.

The function render_factor_models_statistics has the following arguments:
- data: This data is needed to render the model statistics for.
- template: The respective LaTeX template found in the latex_templates folder.
- variables: The variables to render the model staistics for.
- invert: Whether or not the PDF should be color-inverted (because of dark mode).
The function "render_quintiles" adds the PDF file "cover.pdf" that can be rendered below the code in the main.ipynb file.
"""

__all__ = ["merge_data", "convert_frequency", "quintile_returns", "tertile_returns", "two_times_three_returns", "render_summary_statistics", "render_quintiles", "render_factor_model_statistics"]

import pandas as pd, os, subprocess, easydict, time, numpy as np

# a function that returns the asterisk for the p-values
def asterisk_for_p_value(p_value):
    match p_value:
        case _ if p_value <= 0.01:
            return "***"
        case _ if p_value <= 0.05:
            return "**"
        case _ if p_value <= 0.1:
            return "*"
        case _:
            return ""

# reading all data subsets as data frames
def merge_data(path="", download=True):
    file_names = os.listdir(path + "/coingecko")
    dfs = []
    for file_name in file_names:
        if file_name[-4:] == ".csv" and file_name[-11:] != "cg_data.csv":
            df = pd.read_csv(path + "/coingecko/" + file_name, index_col=["date"])
            dfs.append(df)
    # combining all dataframes in the dfs list
    output = pd.concat(dfs)
    # downloading the merged data set
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

def convert_frequency(data, method, returns=False):
    date_range = pd.date_range(start=min(data.index), end=max(data.index), freq="W-SUN")
    if method == "last":
        data = data.groupby("coin_id", group_keys=True).apply(lambda x: x.resample("W-SUN").last().reindex(date_range)).reset_index(level="coin_id", drop=True)
    elif method == "max":
        data = data.groupby("coin_id", group_keys=True).apply(lambda x: x.resample("W-SUN").max(numeric_only=True).reindex(date_range)).reset_index(level="coin_id", drop=True)
    elif method == "mean":
        data = data.groupby("coin_id", group_keys=True).apply(lambda x: x.resample("W-SUN").mean(numeric_only=True).reindex(date_range)).reset_index(level="coin_id", drop=True)
    # .pct_change() defaults to 0 for missing data
    if returns:
        data["return"] = data.groupby("coin_id", group_keys=False)["price"].pct_change()
    data.index.name = "date"
    return data

# function for the quintiles and the zero-investment long-short strategy
def quintile_returns(df, var):
    df = df.copy()
    # writing our own quintile function since the Pandas function qcut() does not adapt well to the edge cases
    def q_cut(x, var):
        x = x.copy()
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
        x.loc[mask] = quintiles
        return x
    # we drop the rows with missing return values so that the quintiles can be computed more accurately
    df.dropna(subset=["return"], inplace=True)
    # creating a new dataframe with the quintile labels for each coin based on the var for that week
    df["quintile"] = df.groupby("date", group_keys=False)[var].transform(lambda x: q_cut(x, var))
    # shifting the quintile information one period into the future
    df["quintile"] = df.groupby("coin_id", group_keys=False)["quintile"].shift(1)
    # grouping the dataframe by quintile and date
    grouped_df = df.groupby(["quintile", "date"])
    # computing the market_cap-weighted return for each quintile in the following week
    return_df = grouped_df.apply(lambda x: (x["return"] * x["market_cap"]).sum() / x["market_cap"].sum())
    # resetting the index and renaming the columns
    if isinstance(return_df, pd.Series):
        return_df = return_df.rename("return").reset_index()
    else:
        return_df = return_df.reset_index(level="quintile", drop=True)
        return_df = return_df.reset_index().rename(columns={0:"return"})
    # return_df["date"] = return_df["date"].shift(1)
    # pivoting the dataframe to get the return series for each quintile
    return_df = return_df.pivot(index="date", columns="quintile", values="return")
    # adding back NaN values
    date_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq="W")
    return_df = return_df.reindex(date_range)
    return return_df

def tertile_returns(df, var):
    df = df.copy()
    # adapting the Pandas qcut function for the edge cases
    def q_cut(x, var):
        x = x.copy()
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
        x.loc[mask] = tertile
        return x
    # creating a new dataframe with the tertile labels for each coin based on the var for that week
    df["tertile"] = df.groupby("date", group_keys=False)[var].transform(lambda x: q_cut(x, var))
    # here, we don't shift the quantiles 1 week into the future
    # grouping the dataframe by tertile and date
    grouped_df = df.groupby(["tertile", "date"])
    # computing the market_cap-weighted return for each tertile in the same week
    return_df = grouped_df.apply(lambda x: (x["return"] * x["market_cap"]).sum() / x["market_cap"].sum())
    # resetting the index and renaming the columns
    if isinstance(return_df, pd.Series):
        return_df = return_df.rename("return").reset_index()
    else:
        return_df = return_df.reset_index(level="tertile", drop=True)
        return_df = return_df.reset_index().rename(columns={0:"return"})
    # pivoting the dataframe to get the return series for each tertile
    return_df = return_df.pivot(index="date", columns="tertile", values="return")
    if not return_df.empty:
        # adding back NaN values
        date_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq="W")
        return_df = return_df.reindex(date_range)
    return return_df

def two_times_three_returns(df):
    df = df.copy()
    # adapting the Pandas qcut function for the edge cases
    def q_cut(x):
        x = x.copy()
        if x.isna().all():
            return pd.Series(np.nan * len(x))
        # this step is necessary since qcut sometimes assigns fewer categorites to the data than specified
        # Compute quantiles for all non-missing values
        mask = x.notna()        
        # computing the median
        median_indices = np.digitize(x[mask], np.percentile(x[mask], [50]), right=False)
        median = ["market_cap_q" + str(i + 1) for i in median_indices]
        x.loc[mask] = median
        return x
    # creating a new dataframe with the median labels for each coin based on the market_cap for that week
    # the availability of the three_week_momentum data correlates with the market_cap variable
    # hence, we drop the rows with missing return values (which determines three_week_momentum)
    df.dropna(subset=["return"], inplace=True)
    df["median"] = df.groupby("date", group_keys=False)["market_cap"].transform(lambda x: q_cut(x))
    # here, we don't shift the quantiles 1 week into the future
    # dividing the data set by quantile
    q1_return_df = tertile_returns(df[df["median"] == "market_cap_q1"], "three_week_momentum")
    q2_return_df = tertile_returns(df[df["median"] == "market_cap_q2"], "three_week_momentum")
    return (q1_return_df, q2_return_df)

def render_summary_statistics(daily_data, market_weekly_data, weekly_data, invert):
    # the number of coins per year for which not every return data point is missing
    number_of_coins = daily_data.groupby(daily_data.index.year, group_keys=False).apply(lambda x: x.groupby("coin_id").apply(lambda x: x["price"].notna().sum() > 0).sum())
    # the other statistics
    # group the data by date
    grouped = daily_data.groupby(daily_data.index.year, group_keys=False)
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
        panel_a_rows += f"{year} & {'{:,}'.format(number_of_coins[number_of_coins.index == year].tolist()[0])} & {round(mean_market_caps[mean_market_caps.index == year].tolist()[0] / 1000000, 2):,.2f} & {round(median_market_caps[median_market_caps.index == year].tolist()[0] / 1000000, 2):,.2f} & {round(mean_volumes[mean_volumes.index == year].tolist()[0] / 1000, 2):,.2f} & {round(median_volumes[median_volumes.index == year].tolist()[0] / 1000, 2):,.2f} \\\ "
    panel_a_summary = f"{'{:,}'.format(daily_data['coin_id'].nunique())} & {round(daily_data['market_cap'].mean(skipna=True) / 1000000, 2):,.2f} & {round(daily_data['market_cap'].median(skipna=True) / 1000000, 2):,.2f} & {round(daily_data['total_volume'].mean(skipna=True) / 1000, 2):,.2f} & {round(daily_data['total_volume'].median(skipna=True) / 1000, 2):,.2f}"
    panel_b_market_return = f"{round(np.mean(market_weekly_data['market_return'].dropna()), 3):,.3f} & {round(market_weekly_data['market_return'].dropna().median(), 3):,.3f} & {round(np.std(market_weekly_data['market_return'].dropna()), 3):,.3f} & {round(market_weekly_data['market_return'].dropna().skew(), 3):,.3f} & {round(market_weekly_data['market_return'].dropna().kurtosis(), 3):,.3f}"
    # displaying the summary statistics for the unedited return series
    panel_b_bitcoin_return = f"{round(weekly_data[weekly_data['coin_id'] == 'bitcoin']['old_return'].dropna().mean(), 3):,.3f} & {round(weekly_data[weekly_data['coin_id'] == 'bitcoin']['old_return'].dropna().median(), 3):,.3f} & {round(weekly_data[weekly_data['coin_id'] == 'bitcoin']['old_return'].dropna().std(), 3):,.3f} & {round(weekly_data[weekly_data['coin_id'] == 'bitcoin']['old_return'].dropna().skew(), 3):,.3f} & {round(weekly_data[weekly_data['coin_id'] == 'bitcoin']['old_return'].dropna().kurtosis(), 3):,.3f}"
    panel_b_ripple_return = f"{round(weekly_data[weekly_data['coin_id'] == 'ethereum']['old_return'].dropna().mean(), 3):,.3f} & {round(weekly_data[weekly_data['coin_id'] == 'ethereum']['old_return'].dropna().median(), 3):,.3f} & {round(weekly_data[weekly_data['coin_id'] == 'ethereum']['old_return'].dropna().std(), 3):,.3f} & {round(weekly_data[weekly_data['coin_id'] == 'ethereum']['old_return'].dropna().skew(), 3):,.3f} & {round(weekly_data[weekly_data['coin_id'] == 'ethereum']['old_return'].dropna().kurtosis(), 3):,.3f}"
    panel_b_ethereum_return = f"{round(weekly_data[weekly_data['coin_id'] == 'ripple']['old_return'].dropna().mean(), 3):,.3f} & {round(weekly_data[weekly_data['coin_id'] == 'ripple']['old_return'].dropna().median(), 3):,.3f} & {round(weekly_data[weekly_data['coin_id'] == 'ripple']['old_return'].dropna().std(), 3):,.3f} & {round(weekly_data[weekly_data['coin_id'] == 'ripple']['old_return'].dropna().skew(), 3):,.3f} & {round(weekly_data[weekly_data['coin_id'] == 'ripple']['old_return'].dropna().kurtosis(), 3):,.3f}"

    # replaceing the predefined strings in the template by the calculates quantities
    template = template.replace("<Panel A rows>", panel_a_rows)
    template = template.replace("<Panel A summary>", panel_a_summary)
    template = template.replace("<Panel B market return>", panel_b_market_return)
    template = template.replace("<Panel B Bitcoin return>", panel_b_bitcoin_return)
    template = template.replace("<Panel B Ripple return>", panel_b_ripple_return)
    template = template.replace("<Panel B Ethereum return>", panel_b_ethereum_return)
    # creating the PDF for the template
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
            asterisk = asterisk_for_p_value(p_value)
            mean_row += f" & {round(mean, 3):.3f}{asterisk}"
            t_row += f" & ({round(t_statstic, 2):.2f})"
        # replaceing the predefined strings in the template by the calculates quantities
        template = template.replace(f"<{var}_mean>", mean_row)
        template = template.replace(f"<{var}_t>", t_row)
    # creating the PDF for the template
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

def render_factor_models_statistics(df, template, vars, invert):
    # opens the LaTeX factor model statistics table template as a string
    template = open(template, "r").read()
    # for the multi-factor model
    if "model" in df.columns:
        for model in df["model"].unique():
            data = df[df["model"] == model]
            for var in vars:
                row_data = data[data["ls_strategy"] == var]
                row = f" & {round(row_data.iloc[0]['alpha'], 3):.3f}"
                # computing the asterisks for the alphas
                asterisk_alpha = asterisk_for_p_value(row_data.iloc[0]["p_alpha"])
                row += f"{asterisk_alpha} & ({round(row_data.iloc[0]['t_alpha'], 2):.2f}) & {round(row_data.iloc[0]['beta_market_excess_return'], 3):.3f}"
                # computing the asterisks for the beta for the market excess return
                asterisk_beta_market_excess_return = asterisk_for_p_value(row_data.iloc[0]["p_beta_market_excess_return"])
                row += f"{asterisk_beta_market_excess_return} & ({round(row_data.iloc[0]['t_beta_market_excess_return'], 2):.2f}) & "
                if model == "model1":
                    # computing the asterisks for the beta for the small-minus-big factor
                    asterisk_beta_small_minus_big = asterisk_for_p_value(row_data.iloc[0]["p_beta_small_minus_big"])
                    # adding 2 empty columns at the end
                    row += f"{round(row_data.iloc[0]['beta_small_minus_big'], 3):.3f}{asterisk_beta_small_minus_big} & ({round(row_data.iloc[0]['t_beta_small_minus_big'], 2):.2f}) & & & "
                if model == "model2":
                    # computing the asterisks for the beta for the momentum factor
                    asterisk_beta_momentum = asterisk_for_p_value(row_data.iloc[0]["p_beta_small_minus_big"])
                    # adding 2 empty columns at the beginning
                    row += " & & " + f"{round(row_data.iloc[0]['beta_momentum'], 3):.3f}{asterisk_beta_momentum} & ({round(row_data.iloc[0]['t_beta_momentum'], 2):.2f}) & "
                if model == "model3":
                    # computing the asterisks for the beta for the small-minus-big factor
                    asterisk_beta_small_minus_big = asterisk_for_p_value(row_data.iloc[0]["p_beta_small_minus_big"])
                    row += f"{round(row_data.iloc[0]['beta_small_minus_big'], 3):.3f}{asterisk_beta_small_minus_big} & ({round(row_data.iloc[0]['t_beta_small_minus_big'], 2):.2f}) & "
                    # computing the asterisks for the beta for the momentum factor
                    asterisk_beta_momentum = asterisk_for_p_value(row_data.iloc[0]["p_beta_small_minus_big"])
                    row += f"{round(row_data.iloc[0]['beta_momentum'], 3):.3f}{asterisk_beta_momentum} & ({round(row_data.iloc[0]['t_beta_momentum'], 2):.2f}) & "
                row += f"{round(row_data.iloc[0]['r_squared'], 3):.3f} & {round(row_data.iloc[0]['mean_absolute_error'], 3):.3f} & {round(row_data.iloc[0]['average_r_squared'], 3):.3f}"
                # adding the row for the variable of interest and the respective model        
                template = template.replace(f"<{var}_data{model[-1]}>", row)
    # when the column model does not exist because we are rendering the one-factor models
    else:
        for var in vars:
            row_data = df[df["ls_strategy"] == var]
            row = f" & {round(row_data.iloc[0]['alpha'], 3):.3f}"
            # computing the asterisks for the alphas
            asterisk_alpha = asterisk_for_p_value(row_data.iloc[0]["p_alpha"])
            row += f"{asterisk_alpha} & ({round(row_data.iloc[0]['t_alpha'], 2):.2f}) & {round(row_data.iloc[0]['beta'], 3):.3f}"
            # computing the asterisks for the betas
            asterisk_beta = asterisk_for_p_value(row_data.iloc[0]["p_beta"])
            row += f"{asterisk_beta} & ({round(row_data.iloc[0]['t_beta'], 2):.2f}) & {round(row_data.iloc[0]['r_squared'], 3):.3f} & {round(row_data.iloc[0]['mean_absolute_error'], 3):.3f} & {round(row_data.iloc[0]['average_r_squared'], 3):.3f}"
            template = template.replace(f"<{var}_data>", row)
    # creating the PDF for the template
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