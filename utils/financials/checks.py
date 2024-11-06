import pandas as pd
from logging import Logger
from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def low_pe(stock_name: str, price_df: pd.DataFrame, eps_df: pd.DataFrame):
    if stock_name in price_df.columns and stock_name in eps_df.columns:
        stock_df = pd.merge(price_df[['Quarter', stock_name]], eps_df[['Quarter', stock_name]], on='Quarter', how='left')
        stock_df["pe"] = stock_df[f"{stock_name}_x"]/stock_df[f"{stock_name}_y"]
        if stock_df["pe"].iloc[-1] > 0:
            return stock_df["pe"].median() < 50
    return None


def increasing_eps(stock_name, eps):
    # return eps[stock_name].iloc[0] > eps[stock_name].iloc[1]
    return eps[stock_name].iloc[0] > 0


def increasing_sales(stock_name, sales):
    sales_list = list(sales[stock_name].values)
    return sales_list[0] > 1.04 * max(sales_list[1:])


def increasing_operating_profit(stock_name, operating_profit):
    return operating_profit[stock_name].iloc[0] > operating_profit[stock_name].iloc[1]


