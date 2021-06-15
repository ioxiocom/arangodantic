import textwrap
from abc import ABC
from functools import lru_cache
from typing import Optional, Type, TypeVar, Union

import aioarangodb.exceptions
import pydantic
from aioarangodb.collection import StandardCollection
from aioarangodb.database import StandardDatabase
from pydantic import Field

from arangodantic.arangdb_error_codes import (
    ERROR_ARANGO_DATA_SOURCE_NOT_FOUND,
    ERROR_ARANGO_DOCUMENT_NOT_FOUND,
    ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED,
)
from arangodantic.configurations import CONF
from arangodantic.cursor import ArangodanticCursor
from arangodantic.exceptions import (
    ConfigError,
    DataSourceNotFound,
    ModelNotFoundError,
    MultipleModelsFoundError,
    UniqueConstraintError,
)
from arangodantic.utils import (
    FilterTypes,
    SortTypes,
    build_filters,
    build_sort,
    remove_whitespace_lines,
)

try:
    from contextlib import asynccontextmanager  # type: ignore
except ImportError:
    from arangodantic.asynccontextmanager import asynccontextmanager

TModel = TypeVar("TModel", bound="Model")


class ArangodanticCollectionConfig(pydantic.BaseModel):
    collection_name: Optional[str] = Field(
        None, description="Override the name of the collection to use"
    )


class Model(pydantic.BaseModel, ABC):
    """
    Base model class.

    Implements basic functionality for Pydantic based models, such as save, delete, get
    etc.
    """

    key_: Optional[str] = Field(alias="_key")
    rev_: Optional[str] = Field(alias="_rev")

    @property
    def id_(self) -> Optional[str]:
        """
        The "_id" field used by ArangoDB (if set).
        """
        if self.key_ is None:
            return None
        return f"{self.get_collection_name()}/{self.key_}"

    @classmethod
    def get_lock_name(cls, key: str) -> str:
        """
        Get the name for the lock for a particular key.
        """
        return f"{CONF.lock_name_prefix}{cls.get_collection_name()}_{key}"

    @classmethod
    def get_lock(cls, key: str):
        """
        Get a named lock corresponding to an instance of this class with a given key.

        :raise ConfigError: Raised if lock is not configured.
        """
        lock_name = cls.get_lock_name(key)
        if not CONF.lock:
            raise ConfigError("Trying to get lock when no lock is configured")
        return CONF.lock(lock_name)

    @classmethod
    async def load(cls: Type[TModel], key: str) -> TModel:
        """
        Get a model based on the ArangoDB "_key".

        :param key: The "_key" of the model to get.
        :return: The model.
        :raise ModelNotFoundError: Raised if no matching document is found.
        """
        response = await cls.get_collection().get(document={"_key": key})
        if response is None:
            raise ModelNotFoundError(f"No '{cls.__name__}' found with _key '{key}'")

        return cls(**response)

    async def reload(self) -> None:
        """
        Reload the model from the database.

        :raise ModelNotFoundError: Raised if the document is not found in the database.
        """
        if not self.key_:
            raise ModelNotFoundError(
                f"Can't reload '{self.__class__.__name__}' without a key"
            )
        new = await self.load(self.key_)
        self.__dict__.update(new.__dict__)

    @classmethod
    @asynccontextmanager
    async def lock_and_load(cls, key):
        """
        Context manager to acquire a lock for a model, load it and return it and upon
        exit release the lock.
        """
        async with cls.get_lock(key):
            yield await cls.load(key)

    @asynccontextmanager
    async def lock_and_reload(self):
        """
        Context manager to lock the current model and reload it from the database.
        Upon exit release the lock.
        """
        async with self.get_lock(self.key_):
            await self.reload()
            yield

    async def save(self, **kwargs) -> None:
        """
        Save the document; either creates a new one or updates/replaces an existing
        document.

        :raise UniqueConstraintViolated: Raised when there is a unique constraint
        violation.
        """

        if not self.rev_:
            # Insert new document
            if not self.key_ and CONF.key_gen:
                # Use generator to generate new key
                self.key_ = str(CONF.key_gen())

            await self.before_save(new=True, **kwargs)

            data = self.get_arangodb_data()
            if not self.key_:
                # Let ArangoDB handle key generation
                del data["_key"]

            try:
                response = await self.get_collection().insert(document=data)
            except aioarangodb.exceptions.DocumentInsertError as ex:
                if ex.error_code == ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED:
                    raise UniqueConstraintError(ex.error_message)
                raise
        else:
            # Update existing document
            await self.before_save(new=False, **kwargs)
            data = self.get_arangodb_data()
            try:
                response = await self.get_collection().replace(document=data)
            except aioarangodb.exceptions.DocumentReplaceError as ex:
                if ex.error_code == ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED:
                    raise UniqueConstraintError(ex.error_message)
                raise

        self.key_ = response["_key"]
        self.rev_ = response["_rev"]

    async def delete(self, ignore_missing=False) -> bool:
        """
        Delete the document.

        :param ignore_missing: Do not raise an exception on missing document.
        :return: Returns False if document was not found and ignore_missing was set to
        True, else always returns True.
        :raise ModelNotFoundError: Raised if the document is not found in the database
        and ignore_missing is set to False (the default value).
        """

        data = self.get_arangodb_data()
        try:
            result: bool = await self.get_collection().delete(
                document=data, silent=True, ignore_missing=ignore_missing
            )
        except aioarangodb.exceptions.DocumentDeleteError as ex:
            if ex.error_code == ERROR_ARANGO_DOCUMENT_NOT_FOUND:
                raise ModelNotFoundError(
                    f"No '{self.__class__.__name__}' found with _key '{self.key_}'"
                )
            raise

        return result

    def get_arangodb_data(self) -> dict:
        """
        Get a dictionary of the data to pass on to ArangoDB when inserting, updating and
        deleting the document.
        """
        data = self.dict(by_alias=True)
        data["_id"] = self.id_
        return data

    @classmethod
    def get_db(cls) -> StandardDatabase:
        return CONF.db

    @classmethod
    @lru_cache()
    def get_collection_name(cls) -> str:
        cls_config: ArangodanticCollectionConfig = getattr(
            cls, "ArangodanticConfig", ArangodanticCollectionConfig()
        )
        if getattr(cls_config, "collection_name", None):
            collection = cls_config.collection_name
        else:
            collection = CONF.collection_generator(cls)  # type: ignore
        return f"{CONF.prefix}{collection}"

    @classmethod
    def get_collection(cls) -> StandardCollection:
        return cls.get_db().collection(cls.get_collection_name())

    @classmethod
    async def ensure_collection(cls, *args, **kwargs):
        """
        Ensure the collection exists and create it if needed.
        """
        name = cls.get_collection_name()
        db = cls.get_db()

        if not await db.has_collection(name):
            await db.create_collection(name, *args, **kwargs)

    @classmethod
    async def delete_collection(cls, ignore_missing: bool = True):
        """
        Delete the collection if it exists.

        :param ignore_missing: Do not raise an exception on missing collection.
        :raise DataSourceNotFound: Raised if the collection does not exist and
        **ignore_missing** is set to False.
        """
        name = cls.get_collection_name()
        db = cls.get_db()

        try:
            return await db.delete_collection(
                name, ignore_missing=ignore_missing, system=False
            )
        except aioarangodb.CollectionDeleteError as ex:
            if ex.error_code == ERROR_ARANGO_DATA_SOURCE_NOT_FOUND:
                raise DataSourceNotFound(
                    f"No collection found with name {cls.get_collection_name()}"
                )
            raise

    @classmethod
    async def truncate_collection(cls, ignore_missing: bool = True) -> bool:
        """
        Truncate the collection if it exists.

        :param ignore_missing: Do not raise an exception on missing collection.
        :return: True if collection was truncated successfully. False if
        **ignore_missing** is set to True and the collection does not exist.
        :raise DataSourceNotFound: Raised if the collection does not exist and
        **ignore_missing** is set to False.
        """
        try:
            await cls.get_collection().truncate()
        except aioarangodb.CollectionTruncateError as ex:
            if ex.error_code == ERROR_ARANGO_DATA_SOURCE_NOT_FOUND:
                if ignore_missing:
                    return False
                else:
                    raise DataSourceNotFound(
                        f"No collection found with name {cls.get_collection_name()}"
                    )
            else:
                raise

        return True

    async def before_save(self, new: bool, **kwargs) -> None:
        """
        Function that's run before saving, should be overridden in subclasses if needed.

        :param new: Tells if the model is new (will be saved for the first time) or not.
        """
        pass

    @classmethod
    async def find(
        cls,
        filters: FilterTypes = None,
        *,
        count: bool = False,
        full_count: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort: SortTypes = None,
    ) -> ArangodanticCursor:
        """
        Find instances of the class using an optional filter and limit.

        :param filters: Filters as a dictionary of either key-values that must match the
        database record or key-expression mappings. E.g. {"name": "John Doe"} or
        {"name": {"!=": "John Doe"}}.
        :param count: If set to True, the total document count is included in
        the result cursor.
        :param full_count: The total number of documents that matched the search
        condition if the limit would not be set.
        :param limit: Limit returned records to a maximum amount.
        :param offset: Offset used when using a limit.
        :param sort: How to sort the results. Can for example be a list of tuples with
        the field name and direction. E.g. [("name", "ASC")].
        """

        # The name we use to refer to the items we're looping over in the AQL FOR loop
        instance_name = "i"

        # List of AQL FILTER expressions that will be added together using "AND" and
        # corresponding bind_vars
        filter_list, bind_vars = build_filters(filters, instance_name=instance_name)

        filter_str = ""
        if filter_list:
            indented_and = "\n        AND "
            filter_str += "FILTER " + indented_and.join(filter_list)

        limit_str = ""
        if limit is not None:
            if offset is None:
                offset = 0
            limit_str += f"LIMIT {int(offset)}, {int(limit)}"
        if offset and limit is None:
            raise ValueError("Offset is only supported together with limit")

        sort_str, sort_bind_vars = build_sort(sort=sort, instance_name=instance_name)
        bind_vars.update(sort_bind_vars)

        query = remove_whitespace_lines(
            textwrap.dedent(
                """
                FOR {instance_name} IN @@collection
                    {filter_str}
                    {sort_str}
                    {limit_str}
                    RETURN {instance_name}
                """
            ).format(
                instance_name=instance_name,
                filter_str=filter_str,
                limit_str=limit_str,
                sort_str=sort_str,
            )
        )
        bind_vars["@collection"] = cls.get_collection_name()

        cursor = await cls.get_db().aql.execute(
            query,
            count=count,
            bind_vars=bind_vars,
            full_count=full_count,
        )
        return ArangodanticCursor(cls, cursor)

    @classmethod
    async def find_one(
        cls,
        filters: FilterTypes = None,
        raise_on_multiple: bool = False,
        *,
        sort: SortTypes = None,
    ):
        """
        Find at most one item matching the optional filters.

        :param filters: Filters in same way as accepted by "find".
        :param raise_on_multiple: Raise an exception if more than one match is found.
        :param sort: Sort in same way as accepted by "find".
        :raises ModelNotFoundError: If no model matched the given filters.
        :raises MultipleModelsFoundError: If "raise_on_multiple" is set to True and more
        than one match is found.
        """
        limit = 1
        if raise_on_multiple:
            limit = 2

        results = await (
            await cls.find(filters=filters, limit=limit, sort=sort)
        ).to_list()
        try:
            if raise_on_multiple and len(results) > 1:
                raise MultipleModelsFoundError(
                    f"Multiple '{cls.__name__}' matched given filters"
                )
            return results[0]
        except IndexError:
            raise ModelNotFoundError(f"No '{cls.__name__}' matched given filters")


class DocumentModel(Model, ABC):
    """
    Base document model class.
    """


class EdgeModel(Model, ABC):
    """
    Base edge model class.
    """

    from_: Union[str, DocumentModel] = Field(alias="_from")
    to_: Union[str, DocumentModel] = Field(alias="_to")

    @property
    def from_key_(self) -> Optional[str]:
        if self.from_ is None:
            return None
        elif isinstance(self.from_, DocumentModel):
            return self.from_.key_
        else:
            return self.from_.rsplit("/", maxsplit=1)[1]

    @property
    def to_key_(self) -> Optional[str]:
        if self.to_ is None:
            return None
        elif isinstance(self.to_, DocumentModel):
            return self.to_.key_
        else:
            return self.to_.rsplit("/", maxsplit=1)[1]

    def get_arangodb_data(self) -> dict:
        """
        Get a dictionary of the data to pass on to ArangoDB when inserting, updating and
        deleting the document.
        """
        data = self.dict(by_alias=True, exclude={"from_", "to_"})
        data["_id"] = self.id_
        if isinstance(self.from_, DocumentModel):
            data["_from"] = self.from_.id_
        else:
            data["_from"] = self.from_

        if isinstance(self.to_, DocumentModel):
            data["_to"] = self.to_.id_
        else:
            data["_to"] = self.to_

        return data

    @classmethod
    async def ensure_collection(cls, *args, **kwargs):
        """
        Ensure the collection exists and create it if needed.
        """
        return await super(EdgeModel, cls).ensure_collection(edge=True, *args, **kwargs)
