import textwrap
from abc import ABC
from contextlib import asynccontextmanager
from datetime import datetime
from functools import lru_cache
from typing import Optional, Sequence, Type, TypeVar

from arango import (
    CollectionDeleteError,
    CollectionTruncateError,
    DocumentDeleteError,
    DocumentInsertError,
    DocumentReplaceError,
)
from arango.collection import StandardCollection
from arango.database import StandardDatabase
from arango.errno import DATA_SOURCE_NOT_FOUND, DOCUMENT_NOT_FOUND
from arango.typings import Json
from asyncer import asyncify
from pydantic import BaseModel, ConfigDict, Field, field_serializer

from arangodantic import (
    CONF,
    ArangodanticCursor,
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

TModel = TypeVar("TModel", bound="Model")


class ArangodanticCollectionConfig(BaseModel):
    collection_name: Optional[str] = Field(
        None, description="Override the name of the collection to use"
    )


class Model(BaseModel, ABC):
    key_: Optional[str] = Field(alias="_key")
    rev_: Optional[str] = Field(alias="_rev", default="")

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
        response = await asyncify(cls.get_collection().get)(document={"_key": key})
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

        if self.rev_ == "":
            # Insert new document
            # and not inspect.isclass(EdgeModel)

            if not self.key_ and CONF.key_gen and not isinstance(self, EdgeModel):
                # Use generator to generate new key
                self.key_ = str(CONF.key_gen())

            await self.before_save(new=True, **kwargs)
            data = self.get_arangodb_data()
            if self.key_ is None:
                # Let ArangoDB handle key generation
                # data.pop("key_", None)
                del data["_id"]
            try:
                response = await asyncify(self.get_collection().insert)(document=data)
            except DocumentInsertError as e:
                if e.error_code == 1210:
                    raise UniqueConstraintError(
                        f"Unique constraint violated for '{self.__class__.__name__}'"
                        f"\n{e.error_message}"
                    ) from e
                raise
        else:
            # Update existing document
            await self.before_save(new=False, **kwargs)
            data = self.get_arangodb_data()
            try:
                response = await asyncify(self.get_collection().update)(document=data)
            except DocumentReplaceError as e:
                if e.error_code == 1210:
                    raise UniqueConstraintError(
                        f"Unique constraint violated for '{self.__class__.__name__}'"
                        f"\n{e.error_message}"
                    ) from e
                raise

        self.key_ = response["_key"]
        self.rev_ = response["_rev"]

    async def before_save(self, new: bool, **kwargs) -> None:
        """
        Function that's run before saving, should be overridden in subclasses if needed.

        :param new: Tells if the model is new (will be saved for the first time) or not.
        """
        pass

    async def upsert(self, **kwargs):
        """
        FIXME
         Wie kann ich hier die history updaten?
         ich will nicht mehrer calls machen
        UPSERT { "_key": @_key }
        INSERT { "_key": @_key , store: @store, history:[] }
        UPDATE { history:PUSH(OLD.history, [@store, DATE_NOW()])}
        in @@collection
        """
        await self.before_save(new=False, **kwargs)
        data = self.get_arangodb_data()
        if self.rev_ == "":
            del data["_rev"]
        del data["_id"]
        if self.key_ is None:
            # Let ArangoDB handle key generation
            del data["_key"]

            # todo hier andere bindvars ohne key_
            print(f"upsert data ohne key {data}")

        else:
            print(f"upsert data {data}")

            bind_vars = {
                "_key": self.key_,
                "@collection": self.get_collection_name(),
            }
            for k, v in data.items():
                bind_vars["new_data"] = {}
                bind_vars["new_data"][k] = v
            print(f"bind_vars: {bind_vars}")
        # try:
        #     await asyncify(self.get_db().aql.execute)()
        # except DocumentInsertError as e:
        #     if e.error_code == 1210:
        #         raise UniqueConstraintError(
        #             f"Unique constraint violated for '{self.__class__.__name__}'"
        #             f"\n{e.error_message}"
        #         ) from e
        #     raise

    @classmethod
    async def insert_many(cls, documents: list[TModel], **kwargs):
        # fixme len_sequence == len(response) is not always true
        #  for loop check if all documents are inserted
        # todo add Expect Handling
        len_sequence = len(documents)
        sequence = []
        print(f"kwargs: {kwargs}")
        for doc in documents:
            sequence.append(doc.get_arangodb_data())
        print(f"sequence: {sequence}")
        try:
            response = await asyncify(cls.get_collection().insert_many)(
                documents=sequence, **kwargs
            )
        except DocumentInsertError as e:
            if e.error_code == 1210:
                raise UniqueConstraintError(
                    f"Unique constraint violated for '{cls.__name__}'"
                    f"\n{e.error_message}"
                ) from e
            raise
        if len_sequence != len(response):
            raise UniqueConstraintError(
                f"Insert Many failed for Unique constraint "
                f"violated for '{cls.__name__}'"
                f"\nlen documents was:{len_sequence}"
                f"\nlen response was:{len(response)}"
            )
        print(f"many insert response: {response}")

    @classmethod
    async def update_many(cls, documents: list[TModel], **kwargs):
        # todo add Expect Handling
        sequence = []
        for doc in documents:
            sequence.append(doc.get_arangodb_data())
        try:
            response = await asyncify(cls.get_collection().update_many)(
                documents=sequence, **kwargs
            )

        except DocumentInsertError as e:
            if e.error_code == 1210:
                raise UniqueConstraintError(
                    f"Unique constraint violated for '{cls.__name__}'"
                    f"\n{e.error_message}"
                ) from e
            raise
        print(f"many update response: {response}")

    @classmethod
    async def get_many(cls, documents: Sequence[str | Json]):
        """
        Return multiple documents ignoring any missing ones.

        :param documents: List of document keys, IDs or bodies. Document bodies
            must contain the "_id" or "_key" fields.
        :type documents: [str | dict]
        :param allow_dirty_read: Allow reads from followers in a cluster.
        :type allow_dirty_read: bool | None
        :return: Documents. Missing ones are not included.
        :rtype: [dict]
        :raise arango.exceptions.DocumentGetError: If retrieval fails.
        """
        results = await asyncify(cls.get_collection().get_many)(documents=documents)

        return results

    async def delete(self, ignore_missing=False) -> None:
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
            result = await asyncify(self.get_collection().delete)(
                document=data, silent=True, ignore_missing=ignore_missing
            )
        except DocumentDeleteError as ex:
            if ex.error_code == DOCUMENT_NOT_FOUND:
                raise ModelNotFoundError(
                    f"No '{self.__class__.__name__}' found with _key '{self.key_}'"
                ) from ex
            raise

        return result

    @classmethod
    async def ensure_collection(cls, *args, **kwargs):
        """
        Ensure the collection exists and create it if needed.
        """
        name = cls.get_collection_name()
        db = cls.get_db()

        if not await asyncify(db.has_collection)(name):
            await asyncify(db.create_collection)(name, *args, **kwargs)

    @classmethod
    def get_collection(cls) -> StandardCollection:
        return cls.get_db().collection(cls.get_collection_name())

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
            return await asyncify(db.delete_collection)(
                name, ignore_missing=ignore_missing, system=False
            )
        except CollectionDeleteError as ex:
            if ex.error_code == DATA_SOURCE_NOT_FOUND:
                raise DataSourceNotFound(
                    f"No collection found with name {name}"
                ) from ex
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
            await asyncify(cls.get_collection().truncate)()
        except CollectionTruncateError as ex:
            if ex.error_code == DATA_SOURCE_NOT_FOUND:
                if ignore_missing:
                    return False
                else:
                    raise DataSourceNotFound(
                        f"No '{cls.__name__}' collection found "
                        f"with name '{cls.get_collection_name()}'"
                    ) from ex
            else:
                raise

        return True

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

        cursor = await asyncify(cls.get_db().aql.execute)(
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
            return cls(**results[0])
        except IndexError:
            raise ModelNotFoundError(f"No '{cls.__name__}' matched given filters")

    @classmethod
    async def all(
        cls, limit: int | None = None, skip: int | None = None
    ) -> list[ArangodanticCursor]:
        """
        Return all documents in the collection.

        :param skip: Number of documents to skip.
        :type skip: int | None
        :param limit: Max number of documents returned.
        :type limit: int | None
        :return: Document cursor list.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentGetError: If retrieval fails.

        """
        cursor = await asyncify(cls.get_collection().all)(limit=limit, skip=skip)
        return await ArangodanticCursor(cls, cursor).to_list()

    @classmethod
    async def keys(cls) -> list[ArangodanticCursor]:
        """
        Return all document keys in the collection.

        :return: Document keys.
        :rtype: list[str]
        :raise arango.exceptions.DocumentGetError: If retrieval fails.

        """
        cursor = await asyncify(cls.get_collection().keys)()
        return await ArangodanticCursor(cls, cursor).to_list()

    @classmethod
    async def ids(cls) -> list[ArangodanticCursor]:
        """
        Return all document ids in the collection.

        :return: Document ids.
        :rtype: list[str]
        :raise arango.exceptions.DocumentGetError: If retrieval fails.

        """
        cursor = await asyncify(cls.get_collection().ids)()
        return await ArangodanticCursor(cls, cursor).to_list()

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

    def get_arangodb_data(self) -> dict:
        """
        Get a dictionary of the data to pass on to ArangoDB when inserting, updating and
        deleting the document.
        """
        data = self.model_dump(by_alias=True)
        data["_id"] = self.id_
        return data


class DocumentModel(Model, ABC):
    """
    Base document model class.
    """

    key_: str = Field(alias="_key", default=None)
    time_created: datetime | None = None
    time_updated: datetime | None = None

    model_config = ConfigDict(ser_json_timedelta="iso8601")

    @field_serializer("time_created", "time_updated")
    def serialize_dt(self, dt: datetime, _info):
        if dt is None:
            return
        if not isinstance(dt, float):
            return int(dt.timestamp() * 1000)
        return int(dt * 1000)

    @classmethod
    async def get_document_attributes_set(cls, attribute: str) -> dict:
        attribute_set = set()
        organisations = await (await cls.find({attribute: {"!=": None}})).to_list()
        for commitments in organisations:
            attribute_set.add(commitments[attribute])
        return {"count": len(attribute_set), "results": attribute_set}

    @classmethod
    async def get_document_attributes(cls, attribute: str, value):
        res = await (await cls.find({attribute: value})).to_list()
        return {"count": len(res), "results": res}


class EdgeModel(Model, ABC):
    """
    Base edge model class.
    Todo add replace and many
    """

    key_: str | None = Field(alias="_key", default=None)
    from_: str | DocumentModel = Field(alias="_from")
    to_: str | DocumentModel = Field(alias="_to")

    # @field_serializer("key_")
    # def serialize_key(self, key, _info):
    #     print(f"info {_info}")
    #     if key is None:
    #         return
    #     elif isinstance(key, uuid.UUID):
    #         return str(uuid)
    #     else:
    #         return key

    @property
    def from_key_(self) -> str | None:
        if self.from_ is None:
            return None
        elif isinstance(self.from_, DocumentModel):
            return self.from_.key_
        else:
            return self.from_.rsplit("/", maxsplit=1)[1]

    @property
    def to_key_(self) -> str | None:
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
        data = self.model_dump(by_alias=True, exclude={"from_", "to_", "_key"})
        # print(f"daaata {data}")
        if self.key_ is None:
            data.pop("_key", None)
        else:
            data["_key"] = self.key_
        if self.rev_ != "":
            data["_id"] = self.id_
        else:
            data["_id"] = None

        if isinstance(self.from_, DocumentModel):
            data["_from"] = self.from_.id_
        else:
            data["_from"] = self.from_

        if isinstance(self.to_, DocumentModel):
            data["_to"] = self.to_.id_
        else:
            data["_to"] = self.to_
        # print(f"daaata edge {data}")
        return data

    @classmethod
    async def ensure_collection(cls, *args, **kwargs):
        """
        Ensure the Edge-collection exists and create it if needed.
        """
        return await super(EdgeModel, cls).ensure_collection(
            edge=True, user_keys=False, *args, **kwargs
        )

    # @classmethod
    # async def insert_many(cls, documents: list):
    #     manys = []
    #     for document in documents:
    #         manys.append(document.get_arangodb_data())
    #     return await super(EdgeModel, cls).insert_many(manys)
