def calculate_rsi(data, window=60):
    """
    Calculates relative strength index
    :param data: a dataframe with the column named as line
    :param window: period over which rsi is calculated
    :return:
    """
    t = data.to_frame()
    t.columns = ['line']
    close_price = t['line']
    delta = close_price.diff()

    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=window, min_periods=1).mean()
    avg_loss = loss.rolling(window=window, min_periods=1).mean()

    rs = avg_gain / avg_loss

    rsi = 100 - (100 / (1 + rs))

    return rsi
