"""
All custom exceptions raise by the package.
"""


class AlreadyExistsException(Exception):
    def __init__(self, msg: str) -> None:
        self.msg = msg
