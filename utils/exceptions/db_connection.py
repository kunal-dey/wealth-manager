class DbConnectionException(Exception):
    """
        To raise custom error when the connection to database fails.
    """

    def __init__(self, msg: str = "An error occurred while connecting to database") -> None:
        self.message = msg
        super().__init__(msg)
