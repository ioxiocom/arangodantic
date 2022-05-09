from typing import List, Optional

import pydantic
from aioarangodb.collection import StandardCollection
from pydantic import Field


class BaseIndex(pydantic.BaseModel):
    """
    Base index model.

    Defines fields common to most of the indexes. The index class should be derived
    from this base class, and add the create_index() function which actually creates
    the index.
    """

    fields: List = Field(..., description="List of fields to index")
    unique: Optional[bool] = Field(
        None, description="Whether the index is unique or not."
    )
    sparse: Optional[bool] = Field(
        None,
        description="If set to True, documents with None in the field are also indexed,"
        " otherwise they're skipped",
    )
    deduplicate: Optional[bool] = Field(
        None,
        description="If set to True, inserting duplicate index values from the same "
        "document triggers unique constraint errors.",
    )
    name: Optional[str] = Field(None, description="Optional name for the index.")
    in_background: Optional[bool] = Field(
        None, description="Do not hold the collection lock."
    )

    async def add_index(self, collection: StandardCollection) -> dict:
        """
        Creates the index on the collection.

        Override this function in the subclass.

        :param collection: The collection to create the index on.
        :return: Dictionary with the index information.
        """
        raise NotImplementedError()


class HashIndex(BaseIndex):
    """
    Creates a hash index on the collection.
    """

    async def add_index(self, collection: StandardCollection):
        return await collection.add_hash_index(
            fields=self.fields,
            unique=self.unique,
            sparse=self.sparse,
            deduplicate=self.deduplicate,
            name=self.name,
            in_background=self.in_background,
        )


class SkiplistIndex(BaseIndex):
    """
    Creates a skiplist index on the collection.
    """

    async def add_index(self, collection: StandardCollection) -> dict:
        return await collection.add_skiplist_index(
            fields=self.fields,
            unique=self.unique,
            sparse=self.sparse,
            deduplicate=self.deduplicate,
            name=self.name,
            in_background=self.in_background,
        )


class GeoIndex(BaseIndex):
    """
    Creates a geo-spatial index on the collection.
    """

    ordered: Optional[bool] = Field(
        None, description="Whether the order is longitude, then latitude."
    )

    async def add_index(self, collection: StandardCollection) -> dict:
        return await collection.add_geo_index(
            fields=self.fields,
            ordered=self.ordered,
            name=self.name,
            in_background=self.in_background,
        )


class FulltextIndex(BaseIndex):
    """
    Creates a fulltext index on the collection.
    """

    min_length: Optional[int] = Field(
        None, description="Minimum number of characters to index."
    )

    async def add_index(self, collection: StandardCollection) -> dict:
        return await collection.add_fulltext_index(
            fields=self.fields,
            min_length=self.min_length,
            name=self.name,
            in_background=self.in_background,
        )


class PersistentIndex(BaseIndex):
    """
    Creates a persistent index on the collection.
    """

    async def add_index(self, collection: StandardCollection) -> dict:
        return await collection.add_persistent_index(
            fields=self.fields,
            unique=self.unique,
            sparse=self.sparse,
            name=self.name,
            in_background=self.in_background,
        )


class TTLIndex(BaseIndex):
    """
    Creates a TTL (time-to-live) index.
    """

    expiry_time: int = Field(
        ..., description="Time of expiry in seconds after document creation."
    )

    async def add_index(self, collection: StandardCollection) -> dict:
        return await collection.add_ttl_index(
            fields=self.fields,
            expiry_time=self.expiry_time,
            name=self.name,
            in_background=self.in_background,
        )
