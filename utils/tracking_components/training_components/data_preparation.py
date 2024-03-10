from logging import Logger

import yfinance as yf
import pandas as pd

from constants.settings import TRAINING_DATE
import numpy as np
from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def generate_data(day_based_data, min_based_data, short=False):

    def get_slope(col):
        index = list(col.index)
        coefficient = np.polyfit(index, col.values, 1)
        ini = coefficient[0]*index[0]+coefficient[1]
        return coefficient[0]/ini

    def position(x):
        """
        given a series it finds whether there was increase of given value eg 1.05
        :param x:
        :return:
        """
        returns = (x.pct_change()+1).cumprod()
        if short:
            return 0 if returns[returns < 0.985].shape[0] == 0 else 1
        else:
            return 0 if returns[returns > 1.02].shape[0] == 0 else 1

    def filtered_single_stock_data(stock_name: str):
        stock_df = min_based_data[[stock_name]].copy()
        stock_df.columns = ['price']
        day_returns = day_based_data[[stock_name]].copy()
        day_returns.columns = ['price']

        # temporary day based df to cache data for min based df
        day_returns['3mo'] = day_returns.reset_index(drop=True).price.rolling(66).apply(get_slope).values
        day_returns['1mo'] = day_returns.reset_index(drop=True).price.rolling(22).apply(get_slope).values
        day_returns['1wk'] = day_returns.reset_index(drop=True).price.rolling(5).apply(get_slope).values
        day_returns['3d'] = day_returns.reset_index(drop=True).price.rolling(3).apply(get_slope).values

        # generating the stock df with necessary input fields
        stock_df['3mo_return'] = stock_df['price'].rolling(1).apply(lambda x: day_returns['3mo'].loc[str(x.index[0].date())])
        stock_df['1mo_return'] = stock_df['price'].rolling(1).apply(lambda x: day_returns['1mo'].loc[str(x.index[0].date())])
        stock_df['1wk_return'] = stock_df['price'].rolling(1).apply(lambda x: day_returns['1wk'].loc[str(x.index[0].date())])
        stock_df['3d_return'] = stock_df['price'].rolling(1).apply(lambda x: day_returns['3d'].loc[str(x.index[0].date())])
        stock_df['1d_return'] = stock_df['price'].reset_index(drop=True).rolling(375).apply(get_slope).values
        stock_df['2hr_return'] = stock_df['price'].reset_index(drop=True).rolling(120).apply(get_slope).values
        stock_df['10m_return'] = stock_df['price'].reset_index(drop=True).rolling(10).apply(get_slope).values
        stock_df['2hr_vol'] = stock_df['price'].rolling(120).apply(lambda x: x.std()/x.iloc[0])
        stock_df['10m_vol'] = stock_df['price'].rolling(10).apply(lambda x: x.std()/x.iloc[0])
        stock_df['1m_shift'] = stock_df.price.shift(1)
        stock_df['dir'] = stock_df['1m_shift'].shift(-375).rolling(375).apply(lambda x: position(x))
        return stock_df.dropna()[['3mo_return', '1mo_return', '1wk_return', '3d_return', '1d_return', '2hr_return', '10m_return', '2hr_vol', '10m_vol', 'dir']].reset_index(drop=True)
    return filtered_single_stock_data


def training_data(non_be_tickers: list, short: bool = False):
    """
    :param short:
    :param non_be_tickers: this should contain the list of all non -BE stocks to start with
    :return:
    """

    # filter the stocks which are having both 6 month data as well 1 wk data

    monthly_stocks = yf.download(tickers=non_be_tickers, interval='1d', period='6mo')
    monthly_stocks.index = pd.to_datetime(monthly_stocks.index)
    monthly_stocks = monthly_stocks.loc[:TRAINING_DATE]
    monthly_stocks = monthly_stocks['Close'].bfill().ffill().dropna(axis=1)

    wk_stocks = yf.download(tickers=non_be_tickers, interval='1m', period='1wk')
    wk_stocks.index = pd.to_datetime(wk_stocks.index, utc=True)
    wk_stocks = wk_stocks.loc[:str(TRAINING_DATE.date())]
    wk_stocks = wk_stocks['Close'].bfill().ffill().dropna(axis=1)

    stocks_list = []
    for a in list(monthly_stocks.columns):
        for b in list(wk_stocks.columns):
            if a == b:
                stocks_list.append(a)
    logger.info(wk_stocks[stocks_list])

    # generating the dataframe having both the input and output
    filtered_single_stock_data = generate_data(monthly_stocks[stocks_list], wk_stocks[stocks_list], short)

    data_df = None
    for st in stocks_list:
        if data_df is not None:
            data_df = pd.concat([data_df, filtered_single_stock_data(st)]).reset_index(drop=True)
        else:
            data_df = filtered_single_stock_data(st)
    return data_df






