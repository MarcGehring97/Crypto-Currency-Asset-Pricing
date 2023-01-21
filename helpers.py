"""
The "merge" function can be used to merge several downloaded data frames into one large data set. The user has to indicate the correct path destination 
where the data subsets are stored. The render functions can be used to create a PDF of the different tables and graphs found in the paper. Each function 
takes in the required data, computes the statistics or time series, and inserts them in the appropriate LaTeX files to ultimately create a PDF file. 
The PDF file can be shown in the main.ipynb file.

The function "merge" has the following arguments:
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "merge" returns a pd dataframe with columns for id, date, price, market_cap, and total_volume.

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

__all__ = ["merge_data", "render_summary_statistics", "render_size_strategy_returns", "render_momentum_strategy_returns"]

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