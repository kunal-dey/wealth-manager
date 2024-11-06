import pandas as pd

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
    market_cap_df['Market_Cap'] = market_cap_df['Market_Cap'] / 1000
    market_cap_df = market_cap_df[(market_cap_df["Market_Cap"] > 0) & (market_cap_df["Market_Cap"] < 5)]
    return market_cap_df
