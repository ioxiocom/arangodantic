from typing import Callable, Optional

from aioarangodb.database import StandardDatabase
from inflection import pluralize, underscore
from pydantic import BaseModel


class Configuration(BaseModel):
    """
    Provide storage for global configurations.
    """

    db: Optional[StandardDatabase] = None
    prefix: str = ""
    key_gen: Optional[Callable] = None
    collection_generator: Optional[Callable] = None
    lock: Optional[Callable] = None
    lock_name_prefix = "arangodantic_"

    class Config:
        arbitrary_types_allowed = True


CONF = Configuration()


def pluralize_underscore(cls: Callable) -> str:
    """
    Pluralize and underscore the name of the class.

    :param cls: The class.
    :return: The pluralized and underscored name of the class.
    """
    return underscore(pluralize(cls.__name__))


def configure(
    db: StandardDatabase,
    prefix: str = "",
    key_gen: Optional[Callable] = None,
    collection_generator: Optional[Callable] = pluralize_underscore,
    lock: Optional[Callable] = None,
) -> None:
    """Configures the DB.

    :param db: The aioarangodb StandardDatabase.
    :param prefix: A prefix to use for collection names.
    :param key_gen: A function that generates new "_key"s.
    :param collection_generator: A function that converts the class to a collection
    name.
    :param lock: A lock class (for example AsyncLock from Shylock) that provides access
    to named locks.
    """
    CONF.db = db
    CONF.prefix = prefix
    CONF.key_gen = key_gen
    CONF.collection_generator = collection_generator
    CONF.lock = lock
