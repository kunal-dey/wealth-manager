from datetime import datetime
import yfinance as yf

from constants.settings import YFINANCE_EXTENSION
from utils.indicators.kaufman_indicator import kaufman_indicator


def filter_stocks(obtained_stock_list):
    # getting all data from yfinance
    initial_stock_list = []
    for symbol in obtained_stock_list:
        if '-BE' not in symbol:
            initial_stock_list.append(symbol)

    monthly_data = yf.download(tickers=[f"{stock}.{YFINANCE_EXTENSION}" for stock in initial_stock_list], period='1y', interval='1d',
                               show_errors=False)['Open']

    monthly_data = monthly_data.bfill().ffill()
    monthly_data = monthly_data.dropna(axis=1)

    # checking whether all the stock follows 2 conditions
    # 1. going from below the medium line to above the medium line
    # 2. touching the minimum line and then increasing
    final_stock_list = []

    for stock_name in list(monthly_data.columns):
        rsi_stock = monthly_data[[stock_name]]
        rsi_stock.insert(1, "line", kaufman_indicator(rsi_stock[stock_name]))
        rsi_stock.insert(2, "max", rsi_stock.line.rolling(window=60).max())
        rsi_stock.insert(3, "min", rsi_stock.line.rolling(window=60).min())
        rsi_stock.insert(4, "med", (8 / 10) * rsi_stock["max"] + (2 / 10) * rsi_stock["min"])
        # going from below the medium line to above the medium line
        check = (rsi_stock["line"] > rsi_stock["med"])
        for index in range(check.shape[0]):
            if index > 1:
                if check.iloc[index] and check.iloc[index - 1] == False:
                    if 6 > datetime.now().weekday() > 0:
                        if (datetime.now() - check.index[index]).days < 2:
                            final_stock_list.append(stock_name)
                    # used for testing on sundays
                    elif datetime.now().weekday() == 6:
                        if (datetime.now() - check.index[index]).days < 3:
                            final_stock_list.append(stock_name)
                    else:
                        if (datetime.now() - check.index[index]).days < 4:
                            final_stock_list.append(stock_name)
    return list(set(final_stock_list))
