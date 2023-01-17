"""
This file can be used to merge several downloaded data frames into one large data set. The user has to
indicate the correct path destination where the data subsets are stored. The output is then stored in
the same directory. The merging and processing takes a long time.

The function "merge" has the following arguments:
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "merge" returns a pd dataframe with columns for id, date, price, market_cap, and total_volume
"""

__all__ = ["render_summary_statistics"]

def render_summary_statistics(start_date, end_date, daily_trading_data, market_returns = ""):

    import io, os, subprocess, easydict, time, pandas as pd, numpy as np

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
    for year in year:
        counter = 0
        market_caps = []
        volumes = []
        for coin in coin_ids:
            coin_daily_prices = coins_daily_prices[coin]
            # if not every market cap is missing in that year for that coin
            if coin_daily_prices[[i[:4] == year for i in list(coin_daily_prices["date"])]]["market_cap"].isna().sum() != 0:
                counter += 1
                market_caps += list(coin_daily_prices[coin_daily_prices["year"] == year]["market_cap"].dropna())
                # NaN values are automatically dropped
                # and if all market caps are missing, the trading volume probably has little meaning
                volumes += list(coin_daily_prices[coin_daily_prices["year"] == year]["volume"].dropna())
        number_of_coins.append(counter)
        mean_market_caps.append(np.mean(market_caps))
        median_market_caps.append(np.median(market_caps))
        mean_volumes.append(np.mean(volumes))
        median_volumes.append(np.median(volumes))

    for i in [years, number_of_coins, mean_market_caps, median_market_caps, mean_volumes, median_volumes]:
        print(i)


    # ripple, bitcoin-2, ethereum-classic



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

    content = convertToLaTeX(df)
    

    args = easydict.EasyDict({})

    with open('cover.tex','w') as f:
        f.write(content%args.__dict__)

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
    if input("Is your editor is dark mode? y/n") in ["Y", "y"]:

        from pdf2image import convert_from_path
        from PIL import ImageChops

        pil_image_lst = convert_from_path(pdf_path)
        image = pil_image_lst[0]
        image = ImageChops.invert(image)
        image.save(pdf_path)
        time.sleep(3)
    
    """

import pandas as pd, datetime
start_date = "2014-01-01"
end_date = str(datetime.date.today())
data_path = r"/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"
# the data was retrieved on 2023-01-13
daily_trading_data = pd.read_csv(data_path+"/cg_data.csv")

render_summary_statistics(start_date, end_date, daily_trading_data)