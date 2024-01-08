from logging import Logger
import numpy as np
import pandas as pd

from utils.logger import get_logger
from utils.tracking_components.training_components.data_preparation import training_data

logger: Logger = get_logger(__name__)


def select_stocks(running_df):
    global logger

    # last_signal_df = running_df.ewm(span=60).mean().reset_index(drop=True).iloc[-60:]
    last_signal_df = running_df.reset_index(drop=True)

    def get_slope(col):
        index = col.index
        st_index = list(index)[0]
        en_index = list(index)[-1]
        coeff = np.polyfit(last_signal_df[col.name].iloc[st_index:en_index].index,
                           last_signal_df[col.name].iloc[st_index:en_index], 1)[0]
        return coeff

    # signal_df = running_df.ewm(span=30).mean()
    # max_df = signal_df.rolling(window=30).max().dropna()
    # min_df = signal_df.rolling(window=30).min().dropna()
    # med_df = (7 / 10) * max_df + (3 / 10) * min_df
    # signal_df = signal_df.iloc[29:]
    # position_df = signal_df > med_df

    max_df = running_df.rolling(window=30).max().dropna()
    min_df = running_df.rolling(window=30).min().dropna()
    med_df = (6 / 10) * max_df + (4 / 10) * min_df
    running_df = running_df.iloc[29:]
    position_df = running_df > med_df
    first_df = position_df.shift(1).iloc[1:]
    result = running_df.iloc[30:][~first_df & position_df.iloc[1:]]
    selected = []

    near_signal_df = running_df.reset_index(drop=True).rolling(window=70).max().dropna().iloc[-420:]
    two_day_signal_df = running_df.reset_index(drop=True).rolling(window=420).min().dropna().iloc[-1080:-420]
    shorter_signal_df = running_df.reset_index(drop=True).rolling(window=15).min().iloc[-80:]
    # shortest_signal_df = running_df.reset_index(drop=True).iloc[-10:]

    near_slopes = near_signal_df.apply(get_slope, axis=0)
    two_slopes = two_day_signal_df.apply(get_slope, axis=0)
    shorter_slopes = shorter_signal_df.apply(get_slope, axis=0)
    # shortest_slopes = shortest_signal_df.apply(get_slope, axis=0)
    coeff_df = pd.DataFrame({
        "price": running_df.iloc[-1],
        "x3": running_df.iloc[-80],
        "x2": running_df.iloc[-420],
        "x1": running_df.iloc[-1080],
        "near_coeff": near_slopes,
        "far_coeff": two_slopes,
        "shorter_coeff": shorter_slopes
        # "shortest_coeff": shortest_slopes
    })
    track_df = coeff_df[(coeff_df.x2 > coeff_df.x3) & (coeff_df.x3 > coeff_df.x1) & (coeff_df.far_coeff > 0) & (
                coeff_df.near_coeff < 0) & (coeff_df.shorter_coeff > 0)]
    track_df.insert(4, "far_per", coeff_df['far_coeff'] / coeff_df['price'])
    growing = list(track_df.sort_values(by='far_per', ascending=False).index)

    for st in list(result.iloc[-1].dropna().index):
        if st in growing:
            selected.append(st)
    return growing


# def train_model(stock_list):
#
#     data_df = training_data([f"{st}.NS" for st in stock_list[:10]])
#     logger.info(data_df)
#
#     train, train_s, test, test_s = split_data(split_ratio=1, data_df=data_df)
#     model = trained_model(train_s, train)
#     return model


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
        running_df['position'] = np.where(running_df['prob'] > 0.8, 1, 0)

        return list(running_df[running_df['position'] == 1].index)

    return predict_stocks
