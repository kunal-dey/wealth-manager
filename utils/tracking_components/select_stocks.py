from logging import Logger
import numpy as np
import pandas as pd

from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def select_stocks(running_df):
    global logger

    # last_signal_df = running_df.ewm(span=60).mean().reset_index(drop=True).iloc[-60:]
    last_signal_df = running_df.reset_index(drop=True)

    def get_slope(col):
        index = col.index
        st_index = list(index)[0]
        en_index = list(index)[-1]
        coeff = np.polyfit(last_signal_df[col.name].iloc[st_index:en_index].index, last_signal_df[col.name].iloc[st_index:en_index], 1)[0]
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
    track_df = coeff_df[(coeff_df.x2 > coeff_df.x3) & (coeff_df.x3 > coeff_df.x1) & (coeff_df.far_coeff > 0) & (coeff_df.near_coeff < 0) & (coeff_df.shorter_coeff > 0)]
    track_df.insert(4, "far_per", coeff_df['far_coeff']/coeff_df['price'])
    growing = list(track_df.sort_values(by='far_per', ascending=False).index)

    for st in list(result.iloc[-1].dropna().index):
        if st in growing:
            selected.append(st)
    return growing
