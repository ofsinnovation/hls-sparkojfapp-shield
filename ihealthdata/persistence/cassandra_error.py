from ihealthdata.exceptions.ihealthexception import ServicesException


class CassandraError(ServicesException):
    """
        This exception class will wrap all exceptions thrown from Cassandra
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message

