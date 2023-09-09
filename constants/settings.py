from datetime import datetime

# time settings
__current_time = datetime.now()
START_TIME = datetime(__current_time.year,__current_time.month,__current_time.day, 9,15,10)
END_TIME = datetime(__current_time.year,__current_time.month,__current_time.day, 15,5)

SLEEP_INTERVAL = 28

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
