class FailException(Exception):
    def __init__(self, message):
        super(FailException, self).__init__(message)


class InvalidReturnValue(Exception):
    def __init__(self, message):
        super(InvalidReturnValue, self).__init__(message)
