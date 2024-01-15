from logging import Logger
import numpy as np
import pandas as pd

from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def predict_running_df(day_based_data, model, params):
    three_month = day_based_data.iloc[-66] / day_based_data.iloc[-1].values
    one_month = day_based_data.iloc[-22] / day_based_data.iloc[-1].values
    one_week = day_based_data.iloc[-5] / day_based_data.iloc[-1].values
    three_days = day_based_data.iloc[-3] / day_based_data.iloc[-1].values
    mu, sigma = params
    mu = mu.iloc[:-1]
    sigma = sigma.iloc[:-1]

    def predict_stocks(min_based_data):
        running_df = pd.concat([
            three_month,
            one_month,
            one_week,
            three_days,
            min_based_data.iloc[-375] / min_based_data.iloc[-1].values,
            min_based_data.iloc[-120] / min_based_data.iloc[-1].values,
            min_based_data.iloc[-10] / min_based_data.iloc[-1].values,
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
        running_df['position'] = np.where((running_df['prob'] > 0.7) & (running_df['prob'] < 0.8), 1, 0)

        return list(running_df[running_df['position'] == 1].index)

    return predict_stocks
