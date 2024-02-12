from datetime import datetime

DEBUG = False

# time settings
__current_time = datetime.now()

TODAY = datetime(2024, 2, 9) if DEBUG else datetime.now()
TRAINING_DATE = datetime(2024, 2, 8) if DEBUG else datetime.now()

GENERATOR_URL = '127.0.0.1:8082'

if DEBUG:
    START_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 0, 1, 0)
    START_BUYING_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 0, 2, 0)
    END_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 23, 59)
    STOP_BUYING_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 23, 58, 0)
    BUY_SHORTS = datetime(__current_time.year, __current_time.month, __current_time.day, 23, 58, 0)
else:
    START_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 9, 15, 0)
    START_BUYING_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 9, 30, 0)
    END_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 15, 28)
    STOP_BUYING_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 15, 0, 0)
    BUY_SHORTS = datetime(__current_time.year, __current_time.month, __current_time.day, 15, 15, 0)

SLEEP_INTERVAL = 1 if DEBUG else 45

# expected returns are set in this section
DELIVERY_INITIAL_RETURN = 0.0003334
DELIVERY_INCREMENTAL_RETURN = 0.006
DAILY_MINIMUM_RETURN = 0.006

INTRADAY_INITIAL_RETURN = 0.00033
INTRADAY_INCREMENTAL_RETURN = 0.006

STARTING_CASH = 60001

EXPECTED_MINIMUM_MONTHLY_RETURN = 0.06  # minimum monthly_return which is expected

# total investment
MAXIMUM_STOCKS = 10
MAXIMUM_ALLOCATION = 10000

END_PROCESS = False


def get_allocation():
    global MAXIMUM_ALLOCATION
    return MAXIMUM_ALLOCATION


def get_max_stocks():
    global MAXIMUM_STOCKS
    return MAXIMUM_STOCKS


def set_max_stocks(max_stocks):
    global MAXIMUM_STOCKS
    MAXIMUM_STOCKS = max_stocks


def end_process():
    """
        function is used to end the process if price generator sends END PROCESS
    :return:
    """
    global END_PROCESS
    return END_PROCESS


def set_end_process(value):
    """
        function is used to end the process if price generator sends END PROCESS
    :param value:
    :return:
    """
    global END_PROCESS
    END_PROCESS = value
