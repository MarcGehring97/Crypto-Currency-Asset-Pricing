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

__all__ = ["merge_data", "convert_frequency", "quintile_returns", "render_summary_statistics", "render_size_strategy_returns", "render_momentum_strategy_returns"]

# reading all data subsets as data frames
def merge(path="", download=True):

    import pandas as pd, os

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

    import pandas as pd

    grouped = data.groupby(data["coin_id"])
    dfs = []
    for group in grouped.groups:
        if method == "last":
            group = grouped.get_group(group).resample("W-SUN").last()
        elif method == "max":
            group = grouped.get_group(group).resample("W-SUN").max()
        if returns:
            # it is important to compute the returns here the function does not recognize the gap between the data sets for the individual coins
            group["return"] = group["price"].pct_change()
        # we need to reindex to add back the missing value
        date_range = pd.date_range(start=min(data.index), end=max(data.index), freq="W")
        group = group.reindex(date_range)
        dfs.append(group)

    # combining all dataframes
    return pd.concat(dfs)

# function for the zero-investment long-short strategy
# it returns a list for the returns of the top 20% and the bottom 20% of a given characteristic
def quintile_returns(size_characteristic, weekly_returns_data):
    first_quintile_returns = []
    second_quintile_returns = []
    third_quintile_returns = []
    fourth_quintile_returns = []
    fifth_quintile_returns = []
    # initializing the list of included coins
    first_quintile_coins = []
    second_quintile_coins = []
    third_quintile_coins = []
    fourth_quintile_coins = []
    fifth_quintile_coins = []
    # taking an arbitrary ID to loop through all weeks

    weekly_returns_data

    for i in coins_weekly_returns[coin_ids[0]].index:
        year = coins_weekly_returns[coin_ids[0]]["year"][i]
        week = coins_weekly_returns[coin_ids[0]]["week"][i]
        if year == 2014 and week == 1:
            # in the first week we have no quintile data (and also no return data)
            first_quintile_returns.append(np.nan)
            second_quintile_returns.append(np.nan)
            third_quintile_returns.append(np.nan)
            fourth_quintile_returns.append(np.nan)
            fifth_quintile_returns.append(np.nan)
        else:
            # computing the returns for the quintiles
            for quintile in ["first", "second", "third", "fourth", "fifth"]:
                match quintile:
                    case "first":
                        coins = first_quintile_coins
                    case "second":
                        coins = second_quintile_coins
                    case "third":
                        coins = third_quintile_coins
                    case "fourth":
                        coins = fourth_quintile_coins
                    case "fifth":
                        coins = fifth_quintile_coins
                returns = []
                market_caps = []
                for coin_id in coins:
                    coin_weekly_returns = weekly_returns_data[weekly_returns_data["coind_id"] == coin_id]
                    coin_weekly_data = coin_weekly_returns[(coin_weekly_returns["year"] == year) & (coin_weekly_returns["week"] == week)]
                    # ignoring all NaNs
                    # the most convenient way to check if no cell value are NaN is by applying .isna().sum().sum()
                    if coin_weekly_data.isna().sum().sum() == 0:
                        returns.append(coin_weekly_data["return"].tolist()[0])
                        market_caps.append(coin_weekly_data["market_cap"].tolist()[0])
                # if all returns are NaN (for example, in the first week of the time period considered)
                if len(returns) == 0:
                    # if no value was added
                    match quintile:
                        case "first":
                            first_quintile_returns.append(np.nan)
                        case "second":
                            second_quintile_returns.append(np.nan)
                        case "third":
                            third_quintile_returns.append(np.nan)
                        case "fourth":
                            fourth_quintile_returns.append(np.nan)
                        case "fifth":
                            fifth_quintile_returns.append(np.nan)
                else:
                    # for every week add the value-weighted market return (the sumproduct of the returns and the market caps divided by the sum of the market caps) and the included coin IDs
                    weighted_average = (sum(x * y for x, y in zip(returns, market_caps)) / sum(market_caps))
                    match quintile:
                        case "first":
                            first_quintile_returns.append(weighted_average)
                        case "second":
                            second_quintile_returns.append(weighted_average)
                        case "third":
                            third_quintile_returns.append(weighted_average)
                        case "fourth":
                            fourth_quintile_returns.append(weighted_average)
                        case "fifth":
                            fifth_quintile_returns.append(weighted_average)

        # computing quintiles for the next week 
        characteristic_data = []
        coin_ids_copy = coin_ids.copy().tolist()
        for coin_id in coin_ids:
            # the size characteristic for every coin
            characteristic_data = coins_weekly_returns[coin_id][(coins_weekly_returns[coin_id]["year"] == year) & (coins_weekly_returns[coin_id]["week"] == week)][size_characteristic]
            if not pd.isna(characteristic_data.tolist()[0]):
                characteristic_data.append(characteristic_data.tolist()[0])
            else:
                # ignoring all coins with missing values
                coin_ids_copy.remove(coin_id)
        # finding the coin ids of the top and bottom 20% market cap coins
        characteristic_data = pd.DataFrame({"coin_id": coin_ids_copy, size_characteristic: characteristic_data})
        first_quintile = characteristic_data.quantile(q=0.2, interpolation="nearest").tolist()[0]
        second_quintile = characteristic_data.quantile(q=0.4, interpolation="nearest").tolist()[0]
        third_quintile = characteristic_data.quantile(q=0.6, interpolation="nearest").tolist()[0]
        fourth_quintile = characteristic_data.quantile(q=0.8, interpolation="nearest").tolist()[0]
        # and the according coins
        first_quintile_coins = characteristic_data["coin_id"][characteristic_data[size_characteristic] <= first_quintile].tolist()
        second_quintile_coins = characteristic_data["coin_id"][(characteristic_data[size_characteristic] > first_quintile) & (characteristic_data[size_characteristic] <= second_quintile)].tolist()
        third_quintile_coins = characteristic_data["coin_id"][(characteristic_data[size_characteristic] > second_quintile) & (characteristic_data[size_characteristic] <= third_quintile)].tolist()
        fourth_quintile_coins = characteristic_data["coin_id"][(characteristic_data[size_characteristic] > third_quintile) & (characteristic_data[size_characteristic] <= fourth_quintile)].tolist()
        fifth_quintile_coins = characteristic_data["coin_id"][characteristic_data[size_characteristic] > fourth_quintile].tolist()

    return {"first": first_quintile_returns, "second": second_quintile_returns, "third": third_quintile_returns, "fourth": fourth_quintile_returns, "fifth": fifth_quintile_returns}

def render_summary_statistics(daily_trading_data, market_weekly_returns, weekly_returns_data, invert):

    import os, subprocess, easydict, time, numpy as np
    
    # group the data by date
    grouped = daily_trading_data.groupby(daily_trading_data.index.year)
    # the number of coins per year for which not every return data point is missing
    number_of_coins = {}
    for year in daily_trading_data.index.year.dropna().unique():
        sub_df = daily_trading_data[daily_trading_data.index.year == year]
        counter = 0
        for coin_id in sub_df["coin_id"].dropna().unique():
            # when at least one return data point is not NaN
            if sub_df[sub_df["coin_id"] == coin_id]["return"].notna().sum() > 0:
                counter += 1
        number_of_coins[year] = counter
    # the other statistics
    mean_market_caps = grouped.apply(lambda x: (x["market_cap"].mean())))
    median_market_caps = grouped.apply(lambda x: (x["market_cap"].median()))
    mean_volumes = grouped.apply(lambda x: (x["total_volume"].mean()))
    median_volumes = grouped.apply(lambda x: (x["total_volume"].median()))
    
    # opens the LaTeX summary statistics table template as a string
    template = open("latex_templates/summary_statistics.tex", "r").read()

    # adding the rows to the LaTeX template
    panel_a_rows = ""
    for year in daily_trading_data.index.year.unique().dropna():
        panel_a_rows += f"{year} & {round(number_of_coins[year], 2):,} & {round(mean_market_caps[mean_market_caps.index.year == year] / 1000000, 2):,} & {round(median_market_caps[median_market_caps.index.year == year] / 1000000, 2):,} & {round(mean_volumes[mean_volumes.index.year == year] / 1000, 2):,} & {round(median_volumes[median_volumes.index.year == year] / 1000, 2):,} \\\ "

    panel_a_summary = f"{len(daily_trading_data['coin_id'].unique().sum())} & {round(np.mean(daily_trading_data['market_cap']) / 1000000, 2):,} & {round(np.median(daily_trading_data['market_cap']) / 1000000, 2):,} & {round(np.mean(daily_trading_data['total_volume']) / 1000, 2):,} & {round(np.median(daily_trading_data['total_volume']) / 1000, 2):,}"
    panel_b_market_return = f"{round(market_weekly_returns.dropna().mean(), 3):,} & {round(market_weekly_returns.dropna().median(), 3):,} & {round(market_weekly_returns.dropna().std(), 3):,} & {round(market_weekly_returns.dropna().skew(), 3):,} & {round(market_weekly_returns.dropna().kurtosis(), 3):,}"
    panel_b_bitcoin_return = f"{round(weekly_returns_data[weekly_returns_data['coin_id'] == 'bitcoin']['return'].dropna().mean(), 3):,} & {round(weekly_returns_data[weekly_returns_data['coin_id'] == 'bitcoin']['return'].dropna().median(), 3):,} & {round(weekly_returns_data[weekly_returns_data['coin_id'] == 'bitcoin']['return'].dropna().std(), 3):,} & {round(weekly_returns_data[weekly_returns_data['coin_id'] == 'bitcoin']['return'].dropna().skew(), 3):,} & {round(weekly_returns_data[weekly_returns_data['coin_id'] == 'bitcoin']['return'].dropna().kurtosis(), 3):,}"
    panel_b_ripple_return = f"{round(weekly_returns_data[weekly_returns_data['coin_id'] == 'ethereum']['return'].dropna().mean(), 3):,} & {round(weekly_returns_data[weekly_returns_data['coin_id'] == 'ethereum']['return'].dropna().median(), 3):,} & {round(weekly_returns_data[weekly_returns_data['coin_id'] == 'ethereum']['return'].dropna().std(), 3):,} & {round(weekly_returns_data[weekly_returns_data['coin_id'] == 'ethereum']['return'].dropna().skew(), 3):,} & {round(weekly_returns_data[weekly_returns_data['coin_id'] == 'ethereum']['return'].dropna().kurtosis(), 3):,}"
    panel_b_ethereum_return = f"{round(weekly_returns_data[weekly_returns_data['coin_id'] == 'ripple']['return'].dropna().mean(), 3):,} & {round(weekly_returns_data[weekly_returns_data['coin_id'] == 'ripple']['return'].dropna().median(), 3):,} & {round(weekly_returns_data[weekly_returns_data['coin_id'] == 'ripple']['return'].dropna().std(), 3):,} & {round(weekly_returns_data[weekly_returns_data['coin_id'] == 'ripple']['return'].dropna().skew(), 3):,} & {round(weekly_returns_data[weekly_returns_data['coin_id'] == 'ripple']['return'].dropna().kurtosis(), 3):,}"

    template = template.replace("<Panel A rows>", panel_a_rows)
    template = template.replace("<Panel A summary>", panel_a_summary)
    template = template.replace("<Panel B market return>", panel_b_market_return)
    template = template.replace("<Panel B Bitcoin return>", panel_b_bitcoin_return)
    template = template.replace("<Panel B Ripple return>", panel_b_ripple_return)
    template = template.replace("<Panel B Ethereum return>", panel_b_ethereum_return)

    args = easydict.EasyDict({})

    with open('cover.tex','w') as f:
        f.write(template%args.__dict__)

    cmd = ['pdflatex', '-interaction', 'nonstopmode', 'cover.tex']
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    proc.communicate()

    retcode = proc.returncode
    if not retcode == 0:
        os.unlink('cover.pdf')
        raise ValueError('Error {} executing command: {}'.format(retcode, ' '.join(cmd))) 

    os.unlink('cover.tex')
    os.unlink('cover.log')
    os.unlink('cover.aux')

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

def render_size_strategy_returns(long_short_data, invert):

    import os, subprocess, easydict, time
    from scipy.stats import ttest_1samp

    # opens the LaTeX summary statistics table template as a string
    template = open("latex_templates/size_strategy_returns.tex", "r").read()

    for size_characteristic in ["log_market_cap", "log_price", "log_max_price", "age"]:
        mean_row = ""
        t_row = ""
        for quintile in ["first", "second", "third", "fourth", "fifth", "_excess_ls"]:
            if quintile == "_excess_ls":
                # also adding the value for the long-short strategy, 5-1
                data = long_short_data[size_characteristic + quintile]
            else:
                data = long_short_data[size_characteristic + "_" + quintile + "_quintile_return"]
            # computing the statistics
            mean = data.mean(skipna=True)
            # per default, this function performs a two-sided t-test
            t_test = ttest_1samp(data, 0, nan_policy="omit")
            t_statstic = t_test[0]
            p_value = t_test[1]
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
                
        template = template.replace("<" + size_characteristic + "_mean>", mean_row)
        template = template.replace("<" + size_characteristic + "_t>", t_row)

    args = easydict.EasyDict({})

    with open('cover.tex','w') as f:
        f.write(template%args.__dict__)

    cmd = ['pdflatex', '-interaction', 'nonstopmode', 'cover.tex']
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    proc.communicate()

    retcode = proc.returncode
    if not retcode == 0:
        os.unlink('cover.pdf')
        raise ValueError('Error {} executing command: {}'.format(retcode, ' '.join(cmd))) 

    os.unlink('cover.tex')
    os.unlink('cover.log')
    os.unlink('cover.aux')

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

def render_momentum_strategy_returns(long_short_data, invert):

    import os, subprocess, easydict, time
    from scipy.stats import ttest_1samp

    # opens the LaTeX summary statistics table template as a string
    template = open("latex_templates/momentum_strategy_returns.tex", "r").read()

    for momentum_return_series in ["one_week_momentum", "two_week_momentum", "three_week_momentum", "four_week_momentum", "one_to_four_week_momentum", "eight_week_momentum", "sixteen_week_momentum", "fifty_week_momentum", "one_hundred_week_momentum"]:
        mean_row = ""
        t_row = ""
        for quintile in ["first", "second", "third", "fourth", "fifth", "_excess_ls"]:
            if quintile == "_excess_ls":
                # also adding the value for the long-short strategy, 5-1
                data = long_short_data[momentum_return_series + quintile]
            else:
                data = long_short_data[momentum_return_series + "_" + quintile + "_quintile_return"]
            # computing the statistics
            mean = data.mean(skipna=True)
            # per default, this function performs a two-sided t-test
            t_test = ttest_1samp(data, 0, nan_policy="omit")
            t_statstic = t_test[0]
            p_value = t_test[1]
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
                
        template = template.replace("<" + momentum_return_series + "_mean>", mean_row)
        template = template.replace("<" + momentum_return_series + "_t>", t_row)

    args = easydict.EasyDict({})

    with open('cover.tex','w') as f:
        f.write(template%args.__dict__)

    cmd = ['pdflatex', '-interaction', 'nonstopmode', 'cover.tex']
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    proc.communicate()

    retcode = proc.returncode
    if not retcode == 0:
        os.unlink('cover.pdf')
        raise ValueError('Error {} executing command: {}'.format(retcode, ' '.join(cmd))) 

    os.unlink('cover.tex')
    os.unlink('cover.log')
    os.unlink('cover.aux')

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