import logging

from logging import FileHandler, Formatter, Logger


def get_logger(module_name: str) -> Logger:
    """
    it creates a logger object with the given file name and sets the format used to display in log files
    :param module_name: name of the file where its used
    :return: Logger object having file where it will be stored and format used to store
    """
    logger: Logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)

    formatter: Formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(name)s: %(message)s')

    file_handler: FileHandler = logging.FileHandler('temp/stock_action.log')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger
