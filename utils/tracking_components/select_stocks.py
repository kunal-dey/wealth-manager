from logging import Logger
import numpy as np
import pandas as pd

from utils.indicators.kaufman_indicator import kaufman_indicator
from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def predict_running_df(day_based_data, model, params, short: bool = False):

    def get_slope(col):
        index = list(col.index)
        coefficient = np.polyfit(index, col.values, 1)
        ini = coefficient[0]*index[0]+coefficient[1]
        return coefficient[0]/ini

    three_month = day_based_data.reset_index(drop=True).iloc[-66:].apply(get_slope)
    one_month = day_based_data.reset_index(drop=True).iloc[-22:].apply(get_slope)
    one_week = day_based_data.reset_index(drop=True).iloc[-5:].apply(get_slope)
    three_days = day_based_data.reset_index(drop=True).iloc[-3:].apply(get_slope)

    mu, sigma = params
    mu = mu.iloc[:-1]
    sigma = sigma.iloc[:-1]

    def predict_stocks(min_based_data):

        running_df = pd.concat([
            three_month,
            one_month,
            one_week,
            three_days,
            min_based_data.reset_index(drop=True).iloc[-375:].apply(get_slope),
            min_based_data.reset_index(drop=True).iloc[-120:].apply(get_slope),
            min_based_data.reset_index(drop=True).iloc[-10:].apply(get_slope),
            min_based_data.iloc[-120:].std() / min_based_data.iloc[-120],
            min_based_data.iloc[-10:].std() / min_based_data.iloc[-10]
        ], axis=1)
        running_df.columns = [
            '3mo_return',
            '1mo_return',
            '1wk_return',
            '3d_return',
            '1d_return',
            '2hr_return',
            '10m_return',
            '2hr_vol',
            '10m_vol'
        ]
        running_df.dropna(inplace=True)
        running_df_s = (running_df-mu)/sigma
        running_df['prob'] = model.predict(running_df_s)
        running_df['position'] = np.where(running_df['prob'] > 0.8, 1, 0)

        selected = []
        predictions = list(running_df[running_df['position'] == 1].index)

        filtered_df = min_based_data[predictions]
        line = filtered_df.apply(kaufman_indicator)
        transformed = line.reset_index(drop=True).iloc[-30:].rolling(15).apply(get_slope)
        if short:
            filters = (transformed < transformed.shift(1)) & (transformed.shift(1) < transformed.shift(2)) & (transformed < 0)
        else:
            filters = (transformed > transformed.shift(1)) & (transformed.shift(1) > transformed.shift(2)) & (transformed > 0)

        for st in list(transformed.iloc[-1][filters.iloc[-1]].index):
            if st in predictions:
                selected.append(st)

        return selected

    return predict_stocks
