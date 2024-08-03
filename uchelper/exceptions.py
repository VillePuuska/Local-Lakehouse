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


class DuckDBConnectionSetupError(Exception):
    def __init__(self) -> None:
        self.msg = "Failed to setup DuckDB connection to Unity Catalog when creating this UCClient object."
