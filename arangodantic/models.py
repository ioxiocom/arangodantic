from abc import ABC
from functools import lru_cache
from typing import Optional, Type, TypeVar, Union

import aioarangodb.exceptions
import pydantic
from aioarangodb.collection import StandardCollection
from aioarangodb.database import StandardDatabase
from pydantic import Field

from arangodantic.arangdb_error_codes import (
    ERROR_ARANGO_DOCUMENT_NOT_FOUND,
    ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED,
)
from arangodantic.configurations import CONF
from arangodantic.exceptions import (
    ConfigError,
    ModelNotFoundError,
    UniqueConstraintError,
)

try:
    from contextlib import asynccontextmanager  # type: ignore
except ImportError:
    from arangodantic.asynccontextmanager import asynccontextmanager

TModel = TypeVar("TModel", bound="Model")


class ArangodanticConfig(pydantic.BaseModel):
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
        lock_name = cls.get_lock_name(key)
        if not CONF.lock:
            raise ConfigError("Trying to get lock when no lock is configured")
        async with CONF.lock(lock_name):
            yield await cls.load(key)

    @asynccontextmanager
    async def lock_and_reload(self):
        """
        Context manager to lock the current model and reload it from the database.
        Upon exit release the lock.
        """
        lock_name = self.get_lock_name(self.key_)
        if not CONF.lock:
            raise ConfigError("Trying to get lock when no lock is configured")
        async with CONF.lock(lock_name):
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

            await self._before_save(new=True, **kwargs)

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
            await self._before_save(new=False, **kwargs)
            data = self.get_arangodb_data()
            try:
                response = await self.get_collection().update(
                    document=data, merge=False
                )
            except aioarangodb.exceptions.DocumentUpdateError as ex:
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
        return self.dict(by_alias=True)

    @classmethod
    def get_db(cls) -> StandardDatabase:
        return CONF.db

    @classmethod
    @lru_cache()
    def get_collection_name(cls) -> str:
        cls_config: ArangodanticConfig = getattr(
            cls, "ArangodanticConfig", ArangodanticConfig()
        )
        if cls_config.collection_name:
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
        """
        name = cls.get_collection_name()
        db = cls.get_db()

        return await db.delete_collection(
            name, ignore_missing=ignore_missing, system=False
        )

    async def _before_save(self, new: bool, **kwargs) -> None:
        """
        Function that's run before saving, should be overridden in subclasses if needed.

        :param new: Tells if the model is new (will be saved for the first time) or not.
        """
        pass


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

    def get_arangodb_data(self) -> dict:
        """
        Get a dictionary of the data to pass on to ArangoDB when inserting, updating and
        deleting the document.
        """
        data = self.dict(by_alias=True, exclude={"from_", "to_"})
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
