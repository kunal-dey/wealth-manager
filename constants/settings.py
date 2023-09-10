from datetime import datetime

# time settings
__current_time = datetime.now()
START_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 9, 15, 0)
START_BUYING_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 10, 30, 0)
END_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 15, 13)
STOP_BUYING_TIME = datetime(__current_time.year, __current_time.month, __current_time.day, 15, 10, 0)

SLEEP_INTERVAL = 4

# total investment
MAXIMUM_STOCKS = 3
MAXIMUM_ALLOCATION = 450

END_PROCESS = False
DEBUG = True


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
