class ArangodanticError(Exception):
    """Generic Arangodantic error class."""

    pass


class CursorNotFoundError(ArangodanticError):
    pass


class DataSourceNotFound(ArangodanticError):
    pass


class ModelNotFoundError(ArangodanticError):
    pass


class MultipleModelsFoundError(ArangodanticError):
    pass


class UniqueConstraintError(ArangodanticError):
    pass


class GraphNotFoundError(ArangodanticError):
    pass


class ConfigError(ArangodanticError):
    pass


class CursorError(ArangodanticError):
    pass
