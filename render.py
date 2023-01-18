"""
This file can be used to create a PDF of the different tables and graphs found in the paper. Each function takes in the required data, computes
the statistics or time series, and inserts them in the appropriate LaTeX files to ultimately create a PDF file. The PDF file can be shown in
the main.ipynb file.

The function "render_summary_statistics" has the following arguments:
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- daily_trading_data: This data is needed to compute the statistics for Bitcoin, Ethereum, and Ripple in Panel B
- market_weekly_returns: This data is needed to compute the statistics for the market returns in Panel B
- coins_weekly_returns: This data is needed to compute the statistics for each year in Panel A

The function "render_summary_statistics" adds the PDF file "cover.pdf" that can be rendered below the code in the main.ipynb file.
"""

__all__ = ["render_summary_statistics"]

def render_summary_statistics(start_date, end_date, daily_trading_data, market_weekly_returns, coins_weekly_returns):

    import os, subprocess, easydict, time, pandas as pd, numpy as np

    # replicating the summary statistics for the market cap, volume, and returns as in the paper
    # all unique coin IDs
    coin_ids = pd.unique(daily_trading_data["id"])

    # storing the data in a dict with keys for the ID
    coins_daily_prices = {}
    for coin_id in coin_ids:
        # get all the data for one coin
        coins_daily_prices[coin_id] = daily_trading_data[daily_trading_data["id"] == coin_id]
    
    # the columns of panel A of the table
    start_year = start_date[:4]
    end_year = end_date[:4]
    years = []
    year = start_year
    while int(year) + 1 <= int(end_year):
        years.append(year)
        year = str(int(year) + 1)

    number_of_coins = []
    mean_market_caps = []
    median_market_caps = []
    mean_volumes = []
    median_volumes = []
    all_market_caps = []
    all_volumes = []
    for year in years:
        counter = 0
        market_caps = []
        volumes = []
        for coin in coin_ids:
            coin_daily_prices = coins_daily_prices[coin]
            # if not every market cap is missing in that year for that coin
            if coin_daily_prices[[date[:4] == year for date in list(coin_daily_prices["date"])]]["market_cap"].isna().sum() == 0:
                counter += 1
                market_caps += list(coin_daily_prices[[date[:4] == year for date in list(coin_daily_prices["date"])]]["market_cap"].dropna())
                # NaN values are automatically dropped
                # and if all market caps are missing, the trading volume probably has little meaning
                volumes += list(coin_daily_prices[[date[:4] == year for date in list(coin_daily_prices["date"])]]["total_volume"].dropna())
        number_of_coins.append(counter)
        mean_market_caps.append(np.mean(market_caps))
        median_market_caps.append(np.median(market_caps))
        mean_volumes.append(np.mean(volumes))
        median_volumes.append(np.median(volumes))
        all_market_caps += market_caps
        all_volumes += volumes
    
    # opens the LaTeX summary statistics table template as a string
    template = open("summary_statistics.tex", "r").read()

    # adding the rows to the LaTeX template
    panel_a_rows = ""
    for i in range(len(years)):
        panel_a_rows += years[i] + " & " + str(round(number_of_coins[i], 2)) + " & " + str(round(mean_market_caps[i] / 1000000, 2)) + " & " + str(round(median_market_caps[i] / 1000000, 2)) + " & " + str(round(mean_volumes[i] / 1000, 2)) + " & " + str(round(median_volumes [i] / 1000, 2)) + " \\\ "

    panel_a_summary = str(len(coin_ids)) + " & " + str(round(np.mean(all_market_caps), 2)) + " & " + str(round(np.median(all_market_caps), 2)) + " & " + str(round(np.mean(all_volumes), 2)) + " & " + str(round(np.median(all_volumes), 2))

    panel_b_market_return = str(round(market_weekly_returns["average_return"].dropna().mean(), 3)) + " & " + str(round(market_weekly_returns["average_return"].dropna().median(), 3)) + " & " + str(round(market_weekly_returns["average_return"].dropna().std(), 3)) + " & " + str(round(market_weekly_returns["average_return"].dropna().skew(), 3)) + " & " + str(round(market_weekly_returns["average_return"].dropna().kurtosis(), 3))
    panel_b_bitcoin_return = str(round(coins_weekly_returns["bitcoin"]["return"].dropna().mean(), 3)) + " & " + str(round(coins_weekly_returns["bitcoin"]["return"].dropna().median(), 3)) + " & " + str(round(coins_weekly_returns["bitcoin"]["return"].dropna().std(), 3)) + " & " + str(round(coins_weekly_returns["bitcoin"]["return"].dropna().skew(), 3)) + " & " + str(round(coins_weekly_returns["bitcoin"]["return"].dropna().kurtosis(), 3))
    panel_b_ripple_return = str(round(coins_weekly_returns["ethereum"]["return"].dropna().mean(), 3)) + " & " + str(round(coins_weekly_returns["ethereum"]["return"].dropna().median(), 3)) + " & " + str(round(coins_weekly_returns["ethereum"]["return"].dropna().std(), 3)) + " & " + str(round(coins_weekly_returns["ethereum"]["return"].dropna().skew(), 3)) + " & " + str(round(coins_weekly_returns["ethereum"]["return"].dropna().kurtosis(), 3))
    panel_b_ethereum_return = str(round(coins_weekly_returns["ripple"]["return"].dropna().mean(), 3)) + " & " + str(round(coins_weekly_returns["ripple"]["return"].dropna().median(), 3)) + " & " + str(round(coins_weekly_returns["ripple"]["return"].dropna().std(), 3)) + " & " + str(round(coins_weekly_returns["ripple"]["return"].dropna().skew(), 3)) + " & " + str(round(coins_weekly_returns["ripple"]["return"].dropna().kurtosis(), 3))

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
    if input("Is your editor is dark mode? y/n ") in ["Y", "y"]:

        from pdf2image import convert_from_path
        from PIL import ImageChops

        pil_image_lst = convert_from_path(pdf_path)
        image = pil_image_lst[0]
        image = ImageChops.invert(image)
        image.save(pdf_path)
        time.sleep(3)

"""
import pandas as pd, datetime, random
start_date = "2014-01-01"
end_date = "2023-01-12"

daily_trading_data = pd.DataFrame({"id": [], "date": [], "price": [], "market_cap": [], "total_volume": []})
daily_trading_data["id"] = ["Check"] * 90 + ["Out"] * 90
daily_trading_data["date"] = ["2014"] * 10 + ["2015"] * 10 + ["2016"] * 10 + ["2017"] * 10 + ["2018"] * 10 + ["2019"] * 10 + ["2020"] * 10 + ["2021"] * 10 + ["2022"] * 10 + ["2014"] * 10 + ["2015"] * 10 + ["2016"] * 10 + ["2017"] * 10 + ["2018"] * 10 + ["2019"] * 10 + ["2020"] * 10 + ["2021"] * 10 + ["2022"] * 10
daily_trading_data["price"] = random.sample(range(10, 300), 180)
daily_trading_data["market_cap"] = random.sample(range(10, 300), 180)
daily_trading_data["total_volume"] = random.sample(range(10, 300), 180)

market_weekly_returns = pd.DataFrame({"year": [], "week": [], "average_return": [], "included_ids": []})
market_weekly_returns["year"] = [2014] * 10 + [2015] * 10 + [2016] * 10 + [2017] * 10 + [2018] * 10 + [2019] * 10 + [2020] * 10 + [2021] * 10 + [2022] * 10
market_weekly_returns["week"] = random.sample(range(10, 300), 90)
returns = random.sample(range(10, 300), 90)
returns = [x / 1000 for x in returns]
market_weekly_returns["average_return"] = returns
market_weekly_returns["included_ids"] = [["Test"]] * 90

coins_weekly_returns = {"ethereum": None, "bitcoin": None, "ripple": None, "other": None}
for coin in coins_weekly_returns.keys():
    year = [2014] * 10 + [2015] * 10 + [2016] * 10 + [2017] * 10 + [2018] * 10 + [2019] * 10 + [2020] * 10 + [2021] * 10 + [2022] * 10
    week = random.sample(range(10, 300), 90)
    price = random.sample(range(10, 300), 90)
    market_cap = random.sample(range(10, 300), 90)
    volume = random.sample(range(10, 300), 90)
    returns = random.sample(range(10, 300), 90)
    returns = [x / 1000 for x in returns]

    df = pd.DataFrame({"year": year, "week": week, "price": price, "market_cap": market_cap, "volume": volume, "return": returns})
    coins_weekly_returns[coin] = df
        
# creates a temporary PDF file named "cover.pdf"
# repeating the process overwrites the file
render_summary_statistics(start_date, end_date, daily_trading_data, market_weekly_returns, coins_weekly_returns)
"""

# this function translates a standard dateframe into LaTeX
def convertToLaTeX(df, alignment="c"):
    # this function converts a df dataframe to a LaTeX tabular
    # it prints labels in bold and does not use math mode
    numColumns = df.shape[1]
    numRows = df.shape[0]
    output = io.StringIO()
    colFormat = ("%s|%s" % (alignment, alignment * numColumns))
    #Write header
    output.write("\\documentclass{article}\\begin{document}\\begin{tabular}{%s}\n" % colFormat)
    columnLabels = ["\\textbf{%s}" % label for label in df.columns]
    output.write("& %s\\\\\\hline\n" % " & ".join(columnLabels))
    #Write data lines
    for i in range(numRows):
        output.write("\\textbf{%s} & %s\\\\\n"
                    % (df.index[i], " & ".join([str(val) for val in df.iloc[i]])))
    #Write footer
    output.write("\\pagenumbering{gobble}\\end{tabular}\\end{document}")
    return output.getvalue()