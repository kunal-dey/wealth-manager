import asyncio
from os import getcwd
import re

import pandas as pd

from constants.global_contexts import kite_context


async def get_correct_symbol(lower_price=0, higher_price=400, initial_stock_list=None):
    """
    Some symbol is trade to trade basis so-BE is attached at the end
    :param initial_stock_list: None
    :param lower_price: lowest price above which the stocks are chosen
    :param higher_price: maximum price below which the stocks are chosen
    :return: a list of symbols in correct_format e.g. ['20MICRONS-BE', 'RELIANCE']
    """
    if initial_stock_list is None:
        initial_stock_list = pd.read_csv(getcwd() + "/temp/EQUITY_NSE.csv")[['Symbol']]

    async def get_stocks(sub_list_of_stocks: list):
        """
        Given a list of symbols, it identifies whether there is a BE in symbol or not.
        If the symbol is not removed or has -BE then it is included in output else it is ignored
        :param sub_list_of_stocks: a list of stock symbols
        :return: dictionary with a key as correct stock symbol and value as current stock price
        """
        dict1 = {}
        dict1.update(kite_context.ltp([f"NSE:{stock}" for stock in sub_list_of_stocks]))
        dict1.update(kite_context.ltp([f"NSE:{stock}-BE" for stock in sub_list_of_stocks]))
        return dict1

    # dividing the entire list into a sub blocks of 300 stocks or fewer (for the last one)
    blocks = [(counter * 300, (counter + 1) * 300) for counter in range(int(len(initial_stock_list) / 300))]
    if len(initial_stock_list) % 300 > 0:
        blocks.append((int(len(initial_stock_list) / 300) * 300, len(initial_stock_list)))

    # asynchronously getting the prices for each sub block
    data = await asyncio.gather(*[
        get_stocks(initial_stock_list['Symbol'][block[0]:block[1]]) for block in blocks
    ])

    # one block is one dict of form {'NSE:20MICRONS-BE':234.23,'NSE:RELIANCE':3435.23}
    # hence iterating through one block at a time merging all data into one after removing NSE
    raw_data = {re.split(":", key)[-1]: [block[key]['last_price']] for block in data for key in block.keys()}

    # converting it into csv and filtering the price range
    temp_df = pd.DataFrame(raw_data)
    temp_df = temp_df.transpose()
    temp_df.columns = ['Close']
    temp_df = temp_df[(temp_df['Close'] > lower_price) & (temp_df['Close'] < higher_price)]
    return list(temp_df.index)
