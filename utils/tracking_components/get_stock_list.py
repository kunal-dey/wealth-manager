import pandas as pd
import yfinance as yf

from os import getcwd

from constants.settings import STOCK_NAME_PATH, MARKET_CAP_HEADER_NAME


def filter_penny_stocks():
    """
        function to fiter out the stocks which are penny stocks
    Returns:
        a dataframe with market cap and stock symbols
    """

    market_cap_df = pd.read_csv(getcwd() + STOCK_NAME_PATH)
    market_cap_df.rename(columns={MARKET_CAP_HEADER_NAME: 'Market_Cap'}, inplace=True)
    market_cap_df.dropna(inplace=True)
    market_cap_df['Market_Cap'] = pd.to_numeric(market_cap_df['Market_Cap'], errors='coerce').fillna(0)
    market_cap_df['Market_Cap'] = market_cap_df['Market_Cap'] / 100000
    market_cap_df = market_cap_df[market_cap_df["Market_Cap"] > 0]
    market_cap_df.index = [f"{st}.NS" for st in market_cap_df["Symbol"]]

    stock_list = [f"{st}.NS" for st in market_cap_df["Symbol"].values]
    prediction_df = yf.download(tickers=stock_list, interval='1d', period='1mo', progress=True)

    market_cap_df["price"] = prediction_df["Close"].iloc[-1]
    market_cap_df.dropna(inplace=True)

    # adding filter with market cap less than 5 cr and price less than Rs. 30
    filtering_criteria = (market_cap_df["Market_Cap"] < 5) & (market_cap_df["price"] < 30)
    return market_cap_df[filtering_criteria]
