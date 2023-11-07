from logging import Logger

from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def select_stocks(running_df):
    global logger
    # logger.info(f"df{running_df}")
    signal_df = running_df.ewm(span=60).mean()
    min_df = signal_df.rolling(window=60).min().dropna()
    signal_df = signal_df.iloc[59:]
    # logger.info(f"signal{signal_df}")
    # logger.info(f"min{min_df}")
    position_df = signal_df > min_df * 1.001
    first_df = position_df.shift(1).iloc[1:]
    result = running_df.iloc[60:][~first_df & position_df.iloc[1:]]
    return list(result.iloc[-1].dropna().index)
