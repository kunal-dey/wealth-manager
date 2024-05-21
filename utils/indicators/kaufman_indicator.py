import numpy as np
import pandas as pd


def kaufman_indicator(price: pd.Series, n=5, pow1=1, pow2=20):
    """
    Given a dataframe, it returns the list of the Kaufman indicator values.

    :param price: Price dataframe of the stock
    :param n: number of observations preceding current value
    :param pow1: the fastest period
    :param pow2: the slowest period
    :return: a numpy array with all the calculated kama indicator values
    """
    abs_diffx = abs(price - price.shift(1))
    abs_price_change = np.abs(price - price.shift(n))
    vol = abs_diffx.rolling(n).sum()
    er = abs_price_change / vol
    fastest_sc, slowest_sc = 2 / (pow1 + 1), 2 / (pow2 + 1)

    sc = (er * (fastest_sc - slowest_sc) + slowest_sc) ** 2.0

    answer = np.zeros(sc.size)
    n = len(answer)
    first_value = True
    for i in range(n):
        # if volatility is 0, it turns out to be nan so is considered separately
        if vol[i] == 0:
            answer[i] = answer[i - 1] + 1 * (price[i] - answer[i - 1])
        # this condition is handled if the sc is np.nan
        elif sc[i] != sc[i]:
            answer[i] = np.nan
        else:
            # the first value is the actual value to merge the indicator results fast
            if first_value:
                answer[i] = price[i]
                first_value = False
            else:
                answer[i] = answer[i - 1] + sc[i] * (price[i] - answer[i - 1])
    return answer
