import pandas as pd
from time import sleep
import requests
from bs4 import BeautifulSoup
import re
import yfinance as yf
from constants.settings import TRAINING_DATE, YFINANCE_EXTENSION
from utils.logger import get_logger
from logging import Logger


logger: Logger = get_logger(__name__)


def get_financial_df(stock_list, n):

    def get_quarters():
        current_date = TRAINING_DATE
        quarters = []

        for i in range(10):
            quarter_start = pd.Timestamp(current_date) - pd.offsets.QuarterEnd(startingMonth=3) * i
            if len(quarters) < n:
                quarters.append(quarter_start)
        return quarters

    eps_result = {}
    eps_df = None
    sales_result = {}
    sales_df = None
    operating_profit_result = {}
    operating_profit_df = None
    # try:
    if True:
        for stock in stock_list:
            logger.info(stock)
            sleep(2)
            retries = 0
            response = None
            while retries < 3:
                try:
                    response = requests.get(f"https://www.screener.in/company/{stock}/")
                    break
                except:
                    sleep(2)
                    retries += 1

            if response and response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                quarter = soup.find(id='quarters')

                # to get eps
                eps = quarter.find(string=re.compile("EPS in Rs")).parent.parent
                quarter_list = []
                for t in eps:
                    el = t.text.replace('\n', '')
                    if el:
                        quarter_list.append(t.text)
                remove_name = [float(element.replace(",", "")) for element in quarter_list[1:]]
                eps_ttm = [sum(remove_name[i-3:i+1]) if i+1 > 3 else 0 for i in range(len(remove_name))]

                if len(list(reversed(eps_ttm[-n:]))) != n:
                    logger.info("eps invalid")
                    logger.info(list(reversed(eps_ttm[-n:])))
                else:
                    eps_result[stock] = list(reversed(eps_ttm[-n:]))

                # to get sales
                soup = BeautifulSoup(response.text, 'html.parser')
                quarter = soup.find(id='quarters')

                sales = quarter.find(string=re.compile("Sales"))
                if sales:
                    sales = sales.parent.parent.parent
                else:
                    continue
                quarter_list = []
                for t in sales:
                    el = t.text.replace('\n', '')
                    if el:
                        quarter_list.append(t.text)
                sales_ttm = [float(element.replace(",", "")) for element in quarter_list[1:]]

                if len(list(reversed(sales_ttm[-n:]))) != n:
                    logger.info("sales invalid")
                    logger.info(list(reversed(sales_ttm[-n:])))
                else:
                    sales_result[stock] = list(reversed(sales_ttm[-n:]))

                # to get Operating Profit
                soup = BeautifulSoup(response.text, 'html.parser')
                quarter = soup.find(id='quarters')

                operating_profit = quarter.find(string=re.compile("Operating Profit"))
                if operating_profit:
                    operating_profit = operating_profit.parent.parent
                else:
                    continue
                quarter_list = []
                for t in operating_profit:
                    el = t.text.replace('\n', '')
                    if el:
                        quarter_list.append(t.text)
                operating_profit_ttm = [float(element.replace(",", "")) for element in quarter_list[1:]]

                if len(list(reversed(operating_profit_ttm[-n:]))) != n:
                    logger.info("operating_profit invalid")
                    logger.info(list(reversed(operating_profit_ttm[-n:])))
                else:
                    operating_profit_result[stock] = list(reversed(operating_profit_ttm[-n:]))

        if len(eps_result) > 0:
            eps_df = pd.DataFrame(eps_result).transpose()
            eps_df.columns = get_quarters()
            eps_df = eps_df.transpose()
            eps_df.insert(len(eps_df.columns), 'Unnamed: 0', pd.to_datetime(eps_df.index))
            eps_df.insert(len(eps_df.columns), 'Quarter', eps_df['Unnamed: 0'].dt.to_period('Q'))
            eps_df.drop(['Unnamed: 0'], axis=1)
            eps_df.to_csv(f"temp/financials/eps_df.csv")
        else:
            try:
                eps_df = pd.read_csv(f"temp/financials/eps_df.csv", index_col=0)
            except:
                eps_df = None

        if len(sales_result) > 0:
            sales_df = pd.DataFrame(sales_result).transpose()
            sales_df.columns = get_quarters()
            sales_df = sales_df.transpose()
            sales_df.insert(len(sales_df.columns), 'Unnamed: 0', pd.to_datetime(sales_df.index))
            sales_df.insert(len(sales_df.columns), 'Quarter', sales_df['Unnamed: 0'].dt.to_period('Q'))
            sales_df.drop(['Unnamed: 0'], axis=1)
            sales_df.to_csv(f"temp/financials/sales_df.csv")
        else:
            try:
                sales_df = pd.read_csv(f"temp/financials/sales_df.csv", index_col=0)
            except:
                sales_df = None

        if len(operating_profit_result) > 0:
            operating_profit_df = pd.DataFrame(operating_profit_result).transpose()
            operating_profit_df.columns = get_quarters()
            operating_profit_df = operating_profit_df.transpose()
            operating_profit_df.insert(len(operating_profit_df.columns), 'Unnamed: 0', pd.to_datetime(operating_profit_df.index))
            operating_profit_df.insert(len(operating_profit_df.columns), 'Quarter', operating_profit_df['Unnamed: 0'].dt.to_period('Q'))
            operating_profit_df.drop(['Unnamed: 0'], axis=1)
            operating_profit_df.to_csv(f"temp/financials/operating_profit_df.csv")
        else:
            try:
                operating_profit_df = pd.read_csv(f"temp/financials/operating_profit_df.csv", index_col=0)
            except:
                operating_profit_df = None
    return eps_df, sales_df, operating_profit_df


def get_price_df(stock_list):

    try:
        yfinance_tickers = [f"{stock}.{YFINANCE_EXTENSION}" for stock in stock_list]
        price_df = yf.download(tickers=yfinance_tickers, period='1y', interval='1d')["Close"]
        price_df = price_df.ffill().bfill()
        price_df.index = pd.to_datetime(price_df.index)
        price_df = price_df.loc[:str(TRAINING_DATE.date())]
        logger.info(price_df)
        price_df.columns = [st[:-3] for st in price_df.columns]
        logger.info(price_df)
        price_df.insert(len(price_df.columns), 'Date', pd.to_datetime(price_df.index))
        price_df.insert(len(price_df.columns), 'Quarter', price_df['Date'].dt.to_period('Q'))
        price_df.ffill().bfill(inplace=True)
        price_df.to_csv(f"temp/financials/price_df.csv")
    except:
        try:
            price_df = pd.read_csv(f"temp/financials/price_df.csv", index_col=0)
        except:
            price_df = None
    return price_df
