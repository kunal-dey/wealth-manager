from datetime import datetime

DEBUG = True

# time settings
__current_time = datetime.now()

if DEBUG:
    START_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 00, 15, 0)
    START_BUYING_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 00, 50, 0)
    END_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 23, 55)
    STOP_BUYING_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 23, 40, 0)
else:
    START_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 9, 15, 0)
    START_BUYING_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 10, 30, 0)
    END_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 15, 13)
    STOP_BUYING_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 15, 10, 0)

SLEEP_INTERVAL = 4

# expected returns are set in this section
DELIVERY_INITIAL_RETURN = 0.001
DELIVERY_INCREMENTAL_RETURN = 0.004

INTRADAY_INITIAL_RETURN = 0.001
INTRADAY_INCREMENTAL_RETURN = 0.004

EXPECTED_MINIMUM_MONTHLY_RETURN = 0.05  # minimum monthly_return which is expected

# total investment
MAXIMUM_STOCKS = 3
MAXIMUM_ALLOCATION = 450

END_PROCESS = False


def allocation():
    global MAXIMUM_ALLOCATION
    return MAXIMUM_ALLOCATION


def set_allocation(max_allocation, max_stocks):
    global MAXIMUM_ALLOCATION, MAXIMUM_STOCKS
    MAXIMUM_ALLOCATION = max_allocation
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
