import asyncio
import re

import pandas as pd
import requests

from constants.global_contexts import kite_context
from constants.settings import DEBUG, GENERATOR_URL, CURRENT_STOCK_EXCHANGE, YFINANCE_EXTENSION


async def fetch_current_prices(stock_list):
    """
    Some symbol is trade to trade basis so-BE is attached at the end
    :param stock_list: a list of symbols without NS or NSE
    :return: a dataframe with a current prices for all the stocks with column containing .NS
    """
    initial_stock_list = stock_list

    async def get_stocks(sub_list_of_stocks: list):
        """
        Given a list of symbols, it identifies whether there is a BE in symbol or not.
        If the symbol is not removed or has -BE then it is included in output else it is ignored
        :param sub_list_of_stocks: a list of stock symbols
        :return: dictionary with a key as correct stock symbol and value as current stock price
        """
        dict1 = {}
        dict1.update(kite_context.ltp([f"{CURRENT_STOCK_EXCHANGE}:{stock}" for stock in sub_list_of_stocks]))
        dict1.update(kite_context.ltp([f"{CURRENT_STOCK_EXCHANGE}:{stock}-BE" for stock in sub_list_of_stocks]))
        return dict1

    if DEBUG:
        resp = requests.get(f"http://{GENERATOR_URL}/prices")
        data = resp.json()['data']
        if data == 'ENDED':
            return None
        temp_df = pd.DataFrame({st: [data[st]] for st in data.keys()})
    else:
        # dividing the entire list into a sub blocks of 300 stocks or fewer (for the last one)
        blocks = [(counter * 300, (counter + 1) * 300) for counter in range(int(len(initial_stock_list) / 300))]
        if len(initial_stock_list) % 300 > 0:
            blocks.append((int(len(initial_stock_list) / 300) * 300, len(initial_stock_list)))

        # asynchronously getting the prices for each sub block
        data = await asyncio.gather(*[
            get_stocks(initial_stock_list[block[0]:block[1]]) for block in blocks
        ])

        # one block is one dict of form {'NSE:20MICRONS-BE':234.23,'NSE:RELIANCE':3435.23}
        # hence iterating through one block at a time merging all data into one after removing NSE
        raw_data = {f"{re.split(':', key)[-1]}.{YFINANCE_EXTENSION}": [block[key]['last_price']] for block in data for key in block.keys()}

        # converting it into csv and filtering the price range
        temp_df = pd.DataFrame(raw_data)
    return temp_df
