"""
All custom exceptions raise by the package.
"""


class AlreadyExistsError(Exception):
    def __init__(self, msg: str) -> None:
        self.msg = msg


class DoesNotExistError(Exception):
    def __init__(self, msg: str) -> None:
        self.msg = msg


class UnsupportedOperationError(Exception):
    def __init__(self, msg: str) -> None:
        self.msg = msg


class SchemaMismatchError(Exception):
    def __init__(self, msg: str) -> None:
        self.msg = msg
