from logging import Logger
import numpy as np
import pandas as pd

from utils.indicators.kaufman_indicator import kaufman_indicator
from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def predict_running_df(day_based_data, model, params):

    def get_slope(col):
        index = list(col.index)
        coefficient = np.polyfit(index, col.values, 1)
        ini = coefficient[0]*index[0]+coefficient[1]
        return coefficient[0]/ini

    mu, sigma = params
    mu = mu.iloc[:-1]
    sigma = sigma.iloc[:-1]

    def predict_stocks(min_based_data):

        stocks_df = pd.concat([day_based_data.iloc[:-1], min_based_data.iloc[-2:-1]], ignore_index=True)

        # running_df = pd.concat([
        #     three_month,
        #     one_month,
        #     one_week,
        #     three_days,
        #     min_based_data.reset_index(drop=True).iloc[-375:].apply(get_slope),
        #     min_based_data.reset_index(drop=True).iloc[-120:].apply(get_slope),
        #     min_based_data.reset_index(drop=True).iloc[-10:].apply(get_slope),
        #     min_based_data.iloc[-120:].std() / min_based_data.iloc[-120],
        #     min_based_data.iloc[-10:].std() / min_based_data.iloc[-10]
        # ], axis=1)
        # running_df.columns = [
        #     '3mo_return',
        #     '1mo_return',
        #     '1wk_return',
        #     '3d_return',
        #     '1d_return',
        #     '2hr_return',
        #     '10m_return',
        #     '2hr_vol',
        #     '10m_vol'
        # ]

        col_with_period = {
                '3mo': 66,
                '1mo': 22,
                '1wk': 5,
                '3d': 3
            }

        shifts = [sh for sh in range(5)]
        gen_cols = []

        concat_lst = []
        for shift in shifts:
            for key, val in col_with_period.items():
                gen_cols.append(f"{key}_{shift}")
                concat_lst.append(stocks_df.reset_index(drop=True).iloc[-val:].apply(get_slope))

        running_df = pd.concat(concat_lst, axis=1)
        running_df.columns = gen_cols

        running_df.dropna(inplace=True)
        running_df_s = (running_df-mu)/sigma
        running_df['prob'] = model.predict(running_df_s)
        running_df['position'] = np.where(running_df['prob'] > 0.8, 1, 0)

        selected = []
        predictions = list(running_df[running_df['position'] == 1].index)

        filtered_df = min_based_data[predictions]
        line = filtered_df.apply(kaufman_indicator)
        transformed = line.reset_index(drop=True).iloc[-30:].rolling(15).apply(get_slope)
        filters = (transformed < transformed.shift(1)) & (transformed.shift(1) < transformed.shift(2)) & (transformed < 0)

        for st in list(transformed.iloc[-1][filters.iloc[-1]].index):
            if st in predictions:
                selected.append(st)

        return predictions

    return predict_stocks
